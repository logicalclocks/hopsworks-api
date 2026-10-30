"""`hops factory`: the built-in factories, the project's own, and what a factory definition may say."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest import mock

import pytest
import yaml
from click.testing import CliRunner
from hopsworks.cli import factory_spec, session
from hopsworks.cli.commands import analytics, factory, mlsystem
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
            (
                "options: [daily, weekly]",
                "options: [{value: daily, label: Daily, at 2}]",
            ),
            "options must be",
        ),
        (("kind: Factory", "kind: Factory\nchat: [ask]"), "chat must be text"),
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
        ("build:\n  answers: &a {self: *a}", "not valid YAML"),
        ("a: &l [1, *l]", "contain itself"),
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


@pytest.mark.parametrize(
    "url, provider",
    [
        ("https://github.com/acme/churn", "github"),
        ("git@gitlab.example.com:acme/churn.git", "gitlab"),
        ("https://user@bitbucket.org/acme/churn.git", "bitbucket"),
        ("ssh://git@git.acme.internal/acme/churn", "git"),
    ],
)
def test_an_existing_repository_is_recorded_with_its_provider(url, provider):
    answer = {"create": False, "url": url}
    assert factory_spec.repository_problems(answer, "Repo") == []
    assert factory_spec.repo_record(answer) == {"url": url, "provider": provider}


def test_a_new_repository_needs_no_url_and_an_existing_one_a_clean_url():
    assert factory_spec.repo_record({"create": True}) == {
        "url": "new",
        "provider": "github",
    }
    assert factory_spec.repository_problems({"create": True}, "Repo") == []
    for answer, problem in (
        ({"create": False}, "give the URL"),
        ({"create": False, "url": "churn"}, "is not a git repository URL"),
        (
            {"create": False, "url": "https://me:token@github.com/a/b"},
            "password or token",
        ),
        (
            {"create": False, "url": "https://ghp_TOKEN@github.com/a/b.git"},
            "password or token",
        ),
        ("yes", "must say whether"),
    ):
        assert problem in factory_spec.repository_problems(answer, "Repo")[0]


def test_code_kept_in_hopsworks_needs_no_url_and_creates_no_repository():
    answer = {"create": False, "local": True}
    assert factory_spec.repository_problems(answer, "Repo") == []
    assert factory_spec.repo_record(answer) == {"url": "local", "provider": "local"}
    # The build creates it, so its work tree is trusted like a new GitHub one's.
    assert factory_spec.creates_repo(answer)
    assert (
        "Hopsworks only"
        in factory_spec.repository_problems({"create": False, "local": False}, "Repo")[
            0
        ]
    )


INGEST = """\
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: crm-ingest
title: CRM ingestion
form:
  sections:
    - id: basics
      title: Basics
      fields:
        - {id: slug, type: slug, label: Name, required: true}
        - {id: credentials, type: secrets, label: Credentials, required: true}
phases:
  - {key: ingest, label: Ingest}
build:
  instructions: Ingest the CRM with the credentials named in requirements.credentials.
"""


def test_a_secrets_field_is_top_level_only():
    assert factory_spec.text_problems(INGEST) == []
    nested = INGEST.replace(
        "        - {id: credentials, type: secrets, label: Credentials, required: true}",
        "        - id: sources\n"
        "          type: list\n"
        "          label: Sources\n"
        "          fields:\n"
        "            - {id: credentials, type: secrets, label: Credentials}",
    )
    assert any(
        "cannot be a secrets inside a list" in p
        for p in factory_spec.text_problems(nested)
    )


@pytest.mark.parametrize(
    ("answer", "problem"),
    [
        (["CRM_PASSWORD", "HUBSPOT_TOKEN"], None),
        (None, "Credentials is required."),
        (["crm_password"], "is not an environment variable name"),
        ([{"name": "CRM_PASSWORD", "value": "hunter2"}], "must be a list of"),
        ({"CRM_PASSWORD": "hunter2"}, "must be a list of"),
    ],
)
def test_a_secrets_answer_is_only_variable_names(answer, problem):
    spec = yaml.safe_load(INGEST)
    found = factory_spec.answer_problems(spec, {"slug": "crm", "credentials": answer})
    if problem is None:
        assert found == []
    else:
        assert len(found) == 1 and problem in found[0], found


def test_an_answers_file_with_secret_values_is_refused(
    tmp_path, monkeypatch, logged_in
):
    registered, launched = _recording(monkeypatch, tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(INGEST)
    )
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "slug": "crm",
                "credentials": [{"name": "CRM_PASSWORD", "value": "hunter2"}],
            }
        )
    )
    refused = CliRunner().invoke(
        cli, ["factory", "run", "crm-ingest", "--answers", str(answers)]
    )
    assert refused.exit_code != 0 and "must be a list of" in refused.output
    assert not (tmp_path / "crm").exists()
    answers.write_text(json.dumps({"slug": "crm", "credentials": ["CRM_PASSWORD"]}))
    done = CliRunner().invoke(
        cli, ["factory", "run", "crm-ingest", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "crm" / "system.yaml").read_text())
    assert doc["requirements"] == {"slug": "crm", "credentials": ["CRM_PASSWORD"]}
    assert launched == ["/hops-factory-crm-ingest crm"]


def test_secrets_asked_here_are_saved_privately_and_only_their_names_recorded(
    tmp_path, monkeypatch, logged_in
):
    from hopsworks_common.core import env_var_api

    registered, launched = _recording(monkeypatch, tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(INGEST)
    )
    api = mock.Mock()
    api.get_env_vars.return_value = [mock.Mock()]
    api.get_env_vars.return_value[0].name = "HUBSPOT_TOKEN"
    monkeypatch.setattr(env_var_api, "EnvVarsApi", lambda: api)
    typed = [
        "crm",
        "",  # required: asked again
        "crm_password",  # not a variable name: asked again
        "CRM_PASSWORD",
        "hunter2",
        "HUBSPOT_TOKEN",
        "",  # saved in the account: kept
        "",
    ]
    done = CliRunner().invoke(
        cli, ["factory", "run", "crm-ingest"], input="\n".join(typed) + "\n"
    )
    assert done.exit_code == 0, done.output
    assert "Credentials is required" in done.output
    assert "crm_password is not an environment variable name" in done.output
    assert "HUBSPOT_TOKEN (saved in your account; empty keeps it)" in done.output
    api.set_env_var.assert_called_once_with(
        "CRM_PASSWORD", "hunter2", visibility="PRIVATE"
    )
    text = (tmp_path / "crm" / "system.yaml").read_text()
    assert "hunter2" not in text
    assert yaml.safe_load(text)["requirements"]["credentials"] == [
        "CRM_PASSWORD",
        "HUBSPOT_TOKEN",
    ]


def test_analytics_ingestion_is_a_built_in():
    assert "analytics-ingestion" in factory_spec.BUILTINS


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
    for name in ("mlsystem", "analytics", "create"):
        assert factory.factory_group.get_command(None, name) is None
    for name in ("mlsystem", "analytics", "build"):
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
        "errorCode": 150109,
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
        analytics,
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


def test_a_blueprints_phase_minutes_replace_the_factorys_estimates(
    tmp_path, monkeypatch, logged_in
):
    _recording(monkeypatch, tmp_path)
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "name": "q3-review",
                "question": "why",
                "cadence": "daily",
                "compare": {"weeks": 9},
                # As a preset's fixed answers send them.
                "phase_minutes": {"build": 3},
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "churn-review", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "q3-review" / "system.yaml").read_text())
    assert doc["factory"]["phases"] == [
        {"key": "build", "label": "Build", "minutes": 3},
        {"key": "verify", "label": "Verify"},
    ]
    assert "phase_minutes" not in doc["requirements"]


def test_the_finetune_factory_writes_its_lora_requirements(
    tmp_path, monkeypatch, logged_in
):
    resources = (
        Path(__file__).resolve().parents[4]
        / "hopsworks-ee/hopsworks-common/src/main/resources/factories"
    )
    if not resources.is_dir():
        pytest.skip("hopsworks-ee is not beside this checkout")
    registered, launched = _recording(monkeypatch, tmp_path)
    text = (resources / "ml-finetune.yaml").read_text()
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(text, version=1)
    )
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "slug": "support-qwen",
                "description": "answer support tickets in our tone",
                "model": {"base": "Qwen/Qwen2.5-7B-Instruct"},
                "data": {"source": "files", "dataset": "Resources/tickets.jsonl"},
                "compute": {"runtime": "ray", "workers": 2, "gpus_per_worker": 4},
                "lora": {"quantization": "nf4", "r": 32, "alpha": 64},
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "ml-finetune", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "support-qwen" / "system.yaml").read_text())
    requirements = doc["requirements"]
    assert requirements["model"]["base"] == "Qwen/Qwen2.5-7B-Instruct"
    assert requirements["compute"]["runtime"] == "ray"
    assert requirements["lora"]["quantization"] == "nf4"
    assert requirements["lora"]["r"] == 32
    assert [p["key"] for p in doc["factory"]["phases"]] == [
        "reqs",
        "data",
        "baseline",
        "training",
        "evaluation",
        "serving",
        "app",
        "verify",
    ]
    command = (
        tmp_path
        / "support-qwen"
        / ".claude"
        / "commands"
        / "hops-factory-ml-finetune.md"
    ).read_text()
    assert "**hops-online-inference**" in command
    assert "ray-torch-training-pipeline" in command
    assert launched == ["/hops-factory-ml-finetune support-qwen"]
    assert registered == [("support-qwen", "support-qwen", "ml-finetune")]


def test_the_rag_factory_writes_its_directories_and_retrieval(
    tmp_path, monkeypatch, logged_in
):
    resources = (
        Path(__file__).resolve().parents[4]
        / "hopsworks-ee/hopsworks-common/src/main/resources/factories"
    )
    if not resources.is_dir():
        pytest.skip("hopsworks-ee is not beside this checkout")
    registered, launched = _recording(monkeypatch, tmp_path)
    text = (resources / "ml-rag.yaml").read_text()
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(text, version=1)
    )
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "slug": "policy-search",
                "description": "answer staff questions about our HR policies",
                "documents": {"directories": ["Resources/policies", "Resources/hr"]},
                "embedding": {"model": "BAAI/bge-m3"},
                "retrieval": {"k": "auto"},
                "serving": {"architecture": "one_app"},
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "ml-rag", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "policy-search" / "system.yaml").read_text())
    requirements = doc["requirements"]
    assert requirements["documents"]["directories"] == [
        "Resources/policies",
        "Resources/hr",
    ]
    assert requirements["embedding"]["model"] == "BAAI/bge-m3"
    assert requirements["serving"]["architecture"] == "one_app"
    assert [p["key"] for p in doc["factory"]["phases"]] == [
        "reqs",
        "survey",
        "index",
        "retrieval",
        "agent",
        "app",
        "verify",
    ]
    command = (
        tmp_path / "policy-search" / ".claude" / "commands" / "hops-factory-ml-rag.md"
    ).read_text()
    assert "**hops-agent-deployment**" in command
    assert "find_neighbors" in command
    assert launched == ["/hops-factory-ml-rag policy-search"]
    assert registered == [("policy-search", "policy-search", "ml-rag")]


def test_the_custom_factory_records_the_new_factory_and_its_checks(
    tmp_path, monkeypatch, logged_in
):
    resources = (
        Path(__file__).resolve().parents[4]
        / "hopsworks-ee/hopsworks-common/src/main/resources/factories"
    )
    if not resources.is_dir():
        pytest.skip("hopsworks-ee is not beside this checkout")
    registered, launched = _recording(monkeypatch, tmp_path)
    text = (resources / "custom-factory.yaml").read_text()
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(text, version=1)
    )
    answers = tmp_path / "answers.json"
    answers.write_text(
        json.dumps(
            {
                "slug": "churn-models",
                "description": "predict which customers churn next month",
                "new_factory": {"title": "Churn models", "purpose": ["ml"]},
            }
        )
    )
    done = CliRunner().invoke(
        cli, ["factory", "run", "custom-factory", "--answers", str(answers)]
    )
    assert done.exit_code == 0, done.output
    doc = yaml.safe_load((tmp_path / "churn-models" / "system.yaml").read_text())
    requirements = doc["requirements"]
    assert requirements["system_type"] == "factory"
    assert requirements["new_factory"]["purpose"] == ["ml"]
    assert [p["key"] for p in doc["factory"]["phases"]] == [
        "reqs",
        "design",
        "write",
        "publish",
        "verify",
    ]
    command = (
        tmp_path
        / "churn-models"
        / ".claude"
        / "commands"
        / "hops-factory-custom-factory.md"
    ).read_text()
    assert "**hops-factory**" in command
    assert "hops factory validate factory.yaml" in command
    assert launched == ["/hops-factory-custom-factory churn-models"]
    assert registered == [("churn-models", "churn-models", "custom-factory")]


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        (["Resources/docs", "Jupyter/papers/"], True),
        (["/Projects/p/Resources/docs"], False),
        (["Resources/../../etc"], False),
        (["/"], False),
        ("Resources/docs", False),
    ],
)
def test_a_directories_answer_names_directories_in_the_project(value, ok):
    field = {"id": "dirs", "type": "directories", "label": "Directories"}
    assert (factory_spec._field_problems(field, value, "Directories") == []) == ok


def test_rerunning_the_answers_resumes_with_the_recorded_version(
    tmp_path, monkeypatch, logged_in
):
    registered, launched = _recording(monkeypatch, tmp_path)
    answers = tmp_path / "answers.json"
    answers.write_text(json.dumps({"name": "q3-review", "question": "why"}))
    run = ["factory", "run", "churn-review", "--answers", str(answers)]
    assert CliRunner().invoke(cli, run).exit_code == 0
    newer = REVIEW.replace(
        "Build a {dashboard} over the chosen tables.", "Build something else."
    )
    asked = []

    def get(name, version=None):
        asked.append(version)
        return _definition() if version == 2 else _definition(newer, 3)

    monkeypatch.setattr(factory_api, "_get", get)
    again = CliRunner().invoke(cli, run)
    assert again.exit_code == 0, again.output
    assert 2 in asked
    command = (
        tmp_path / "q3-review" / ".claude" / "commands" / "hops-factory-churn-review.md"
    ).read_text()
    assert "Build a {dashboard} over the chosen tables." in command
    doc = yaml.safe_load((tmp_path / "q3-review" / "system.yaml").read_text())
    assert doc["factory"]["version"] == 2
    assert len(registered) == 1


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
                "system": {"slug": "q3-review"},
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
            yaml.safe_dump(
                {"system": {"slug": "fraud"}, "requirements": {"status": "pending"}}
            )
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
    # The digest of the definition text fetched, which registration sends with the version.
    assert doc["factory"] == {
        "name": "fraud-ml",
        "version": 3,
        "digest": hashlib.sha256(CLONE.encode()).hexdigest(),
        "instructions": "Write a model card for the regulator.",
    }
    assert doc["requirements"] == {"status": "pending", "extra": {"regulator": "FI"}}


def test_the_streaming_factorys_example_builds_with_its_engines(
    tmp_path, monkeypatch, logged_in
):
    resources = (
        Path(__file__).resolve().parents[4]
        / "hopsworks-ee/hopsworks-common/src/main/resources/factories"
    )
    if not resources.is_dir():
        pytest.skip("hopsworks-ee is not beside this checkout")
    text = (resources / "ml-streaming.yaml").read_text()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        factory_api, "_get", lambda name, version=None: _definition(text, version=1)
    )
    seen = {}

    def built_in(ctx, answers, launch):
        seen["answers"] = answers
        target = tmp_path / answers["slug"]
        target.mkdir()
        (target / "system.yaml").write_text(
            yaml.safe_dump({"system": {"slug": answers["slug"]}, "requirements": {}})
        )
        factory_spec.record_factory(ctx.meta.get(factory_spec.META), target)

    monkeypatch.setattr(factory.build, "create", built_in)
    done = CliRunner().invoke(
        cli,
        [
            "factory",
            "run",
            "ml-streaming",
            "--preset",
            "debitcard-example",
            "--no-launch",
        ],
        # Every question offers the preset's answer as its default.
        input="\n" * 40,
    )
    assert done.exit_code == 0, done.output
    answers = seen["answers"]
    assert answers["example"] == "debitcard-example"
    assert answers["system_type"] == "realtime"
    assert answers["reference_code"].endswith("/references/debitcard")
    doc = yaml.safe_load((tmp_path / "debitcard-example" / "system.yaml").read_text())
    assert doc["requirements"]["extra"]["pipelines"] == {
        "batch_cadence": "daily",
        "window_minutes": 60,
    }
    instructions = doc["factory"]["instructions"]
    assert "dbt" in instructions and "Structured Streaming" in instructions


# endregion


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2025-01-01", "2025-01-01T00:00Z"),
        ("2025-01-01T06:30Z", "2025-01-01T06:30Z"),
        ("2025-01-01T08:30+02:00", "2025-01-01T06:30Z"),
        ("2025-01-01 06:30", "2025-01-01T06:30Z"),
        ("yesterday", None),
        ("", None),
    ],
)
def test_a_datetime_answer_is_an_instant_in_utc(value, expected):
    assert factory_spec.utc_instant(value) == expected
    field = {"id": "start", "type": "datetime", "label": "Start"}
    problems = factory_spec._field_problems(field, value or None, "Start")
    assert bool(problems) == (bool(value) and expected is None)


@pytest.mark.parametrize("recorded", ["ml-streaming", "churn-review"])
def test_a_resumed_build_registers_with_the_factory_its_system_yaml_records(
    tmp_path, monkeypatch, recorded
):
    from hopsworks.cli.commands import build

    (tmp_path / "system.yaml").write_text(
        yaml.safe_dump(
            {
                "factory": {"name": recorded, "version": 3, "digest": "d"},
                "system": {"name": "Fraud"},
                "requirements": {"system_type": "realtime"},
            }
        ),
        encoding="utf-8",
    )
    registered = []
    monkeypatch.setattr(
        mlsystem, "register", lambda ctx, path, name, f: registered.append(f)
    )
    # A resumed build has no `hops factory run` context.
    build._register(mock.Mock(meta={}), build._System(tmp_path))
    assert registered == [recorded]


def test_without_a_recorded_factory_the_built_in_name_is_used(tmp_path):
    assert factory_spec.factory_name(None, "ml-batch", tmp_path) == "ml-batch"


def test_trust_records_a_directory_once(tmp_path, monkeypatch):
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(tmp_path))
    system = tmp_path / "fraud"
    system.mkdir()
    factory_spec.trust(system)
    factory_spec.trust(system)
    listing = tmp_path / ".hops" / "claude-trusted-dirs"
    assert listing.read_text().splitlines() == [str(system.resolve())]


@pytest.mark.parametrize(
    ("answer", "new"),
    [
        (None, True),
        ({"create": True}, True),
        ({"create": False, "url": "https://github.com/acme/thirdparty"}, False),
        ("https://github.com/acme/thirdparty", False),
    ],
)
def test_only_a_new_repository_counts_as_created(answer, new):
    assert factory_spec.creates_repo(answer) is new


@pytest.mark.parametrize(
    ("answer", "trusted"),
    [
        (None, True),
        ({"create": False, "url": "https://github.com/acme/thirdparty"}, False),
    ],
)
def test_a_layer_trusts_only_a_work_tree_it_created(
    tmp_path, monkeypatch, answer, trusted
):
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(tmp_path))
    repo = tmp_path / "hops-sales"
    analytics._trust_new(repo, {"repo": answer} if answer else {}, fresh=True)
    listing = tmp_path / ".hops" / "claude-trusted-dirs"
    assert listing.exists() is trusted
    # A work tree that was already there is never added.
    analytics._trust_new(tmp_path / "old", {}, fresh=False)
    assert not listing.exists() or str((tmp_path / "old").resolve()) not in (
        listing.read_text()
    )


@pytest.mark.parametrize(
    ("typed", "expected"),
    [
        ("local", {"create": False, "local": True}),
        ("", {"create": False, "local": True}),
        (
            "https://gitlab.com/acme/churn.git",
            {"create": False, "url": "https://gitlab.com/acme/churn.git"},
        ),
    ],
)
def test_the_repository_prompt_keeps_code_in_hopsworks_on_local(
    monkeypatch, typed, expected
):
    monkeypatch.setattr(factory.click, "confirm", lambda *a, **k: False)
    monkeypatch.setattr(factory.click, "prompt", lambda *a, **k: typed)
    field = {"id": "repo", "type": "repository"}
    assert factory._prompt(field, {"create": False, "local": True}, "Repo") == expected
