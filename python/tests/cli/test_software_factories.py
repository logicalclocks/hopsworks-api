"""The /hops software factories: the command, the agents, and the files the skills ship.

The skills carry programs that end up running in users' ML systems (the system
template, the synthetic data generator, the dashboard program, the app
skeleton), so they are tested here like code, offline.
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import textwrap
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import click
import numpy as np
import pytest
from click.testing import CliRunner
from hopsworks.cli import scaffold
from hopsworks.cli.main import cli


yaml = pytest.importorskip("yaml")

SKILLS = Path(scaffold.__file__).resolve().parents[1] / "skills"
TEMPLATES = Path(scaffold.__file__).resolve().parent / "templates"
REQS = SKILLS / "ml" / "hops-reqs" / "references"
TEMPLATE = REQS / "system_template"
EXAMPLE = REQS / "example-system.yaml"
GENERATOR = SKILLS / "data" / "hops-synthetic-data" / "references" / "generator.py"
DASHBOARD = (
    SKILLS / "dashboards" / "hops-superset" / "references" / "dashboard_program.py"
)
APP = SKILLS / "dashboards" / "hops-app" / "references" / "app_skeleton"
TRINO_JS = SKILLS / "dashboards" / "hops-app" / "references" / "trino_js"
ENTRYPOINTS = [
    TEMPLATE / "src" / "slug_pkg" / "evaluate.py",
    TEMPLATE / "src" / "slug_pkg" / "feature_pipeline.py",
    TEMPLATE / "src" / "slug_pkg" / "training_pipeline.py",
    TEMPLATE / "src" / "slug_pkg" / "inference_pipeline.py",
    TEMPLATE / "benchmarks" / "benchmark_inference.py",
    TEMPLATE / "tests" / "run_integration.py",
    GENERATOR,
]


def _load(path: Path, name: str | None = None):
    spec = importlib.util.spec_from_file_location(name or path.stem, path)
    module = importlib.util.module_from_spec(spec)
    # Registered first, so pydantic can resolve a model's postponed annotations.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _example() -> dict:
    return yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))


def _validate(doc: dict) -> list[str]:
    return _load(TEMPLATE / "tests" / "unit" / "test_system_yaml.py").validate(doc)


# region The command and the agents


COMMANDS = ["hops.md", "hops-ml.md", "hops-build.md", "hops-silver.md", "hops-gold.md"]
AGENTS = {
    # The ML agents inherit the model of /hops-build, the session's own.
    "hops-train-agent": None,
    "hops-infer-agent": None,
    # Spawned from /hops, which runs on Haiku: inheriting would build on Haiku too.
    "hops-dashboard-builder": "opus",
    "hops-app-builder": "opus",
}


def _front(name: str) -> dict:
    return yaml.safe_load(
        (TEMPLATES / name).read_text(encoding="utf-8").split("---")[1]
    )


def test_the_cli_bundle_ships_every_command_and_agent_and_retires_fti():
    files = scaffold.build_files(internal=True, project="p")
    for command in COMMANDS:
        assert f".claude/commands/{command}" in files
    for agent in AGENTS:
        assert f".claude/agents/{agent}.md" in files
    assert not any("hops-fti" in path for path in files)
    assert not (TEMPLATES / "hops-fti.md").exists()


@pytest.mark.parametrize(("name", "model"), sorted(AGENTS.items()))
def test_agent_definitions_use_the_subagent_frontmatter(name, model):
    front = _front(f"{name}.md")
    assert front["name"] == name
    # `tools` is the key Claude Code reads for subagents; `allowed-tools` is ignored there.
    assert "allowed-tools" not in front
    assert set(front["tools"].split(", ")) == {
        "Read",
        "Grep",
        "Glob",
        "Edit",
        "Write",
        "Bash",
    }
    assert front.get("model") == model
    assert "never asks the user" in front["description"]


def test_the_menu_runs_on_haiku_and_hands_every_build_to_a_stronger_agent():
    front = _front("hops.md")
    text = (TEMPLATES / "hops.md").read_text(encoding="utf-8")
    assert front["model"] == "haiku"
    assert "AskUserQuestion" in text
    for section in ("## Explore", "## Status", "## Dashboard", "## App"):
        assert section in text
    assert "hops-dashboard-builder" in text and "hops-app-builder" in text
    assert "`/hops-ml`" in text
    # The menu carries no build instructions of its own.
    for build_step in ("hops app create", "dashboard_program.py", "system_template"):
        assert build_step not in text
    # State the first menu needs is injected, not fetched by a tool call.
    assert "!`ls -d */system.yaml" in text


def test_the_interview_runs_on_haiku_and_records_every_answer_as_it_goes():
    front = _front("hops-ml.md")
    text = (TEMPLATES / "hops-ml.md").read_text(encoding="utf-8")
    assert front["model"] == "haiku"
    assert "AskUserQuestion" in text and "set.py" in text
    for branch in ("### Batch", "### Real-time", "### Agentic", "### Data sources"):
        assert branch in text
    # The interview builds nothing: that is /hops-build, on the session model.
    assert "`/hops-build`" in text
    for agent in AGENTS:
        assert agent not in text
    assert "### train" not in text
    assert "!`hops fg list" in text and "!`hops datasource list" in text


def test_the_first_question_offers_a_description_or_an_example_system():
    text = (TEMPLATES / "hops-ml.md").read_text(encoding="utf-8")
    assert "**Start a new ML system (Recommended)**" in text
    assert "**Build an example ML system**" in text
    labels = {
        slug: entry["label"]
        for slug, entry in yaml.safe_load(
            (REQS / "example-systems.yaml").read_text(encoding="utf-8")
        ).items()
    }
    assert labels == {
        "churn-example": "Churn: which customers will cancel next month (batch)",
        "recs-example": "Personalized recommendations: the products each shopper is likely to buy next (real-time)",
        "gis-example": "GIS military infrastructure finder: outlines aircraft, ships and harbours on a map of Sweden with a pretrained YOLO model (real-time)",
        "helpdesk-example": "Help desk agent: answers support questions from your documents and the customer's recent events (agentic)",
        "run-example": "Hops Run with Kumo Tabular: a racing game flown by NVIDIA's pretrained in-context classifier, served as a deployment (real-time)",
    }
    for slug in labels:
        assert f"`{slug}`" in text
    assert "--example <example>" in text
    build = (TEMPLATES / "hops-build.md").read_text(encoding="utf-8")
    assert "`app.wanted` is recorded, never ask" in build


def test_an_example_system_records_synthetic_data_and_an_app(tmp_path):
    target = _load(REQS / "new_system.py").create(tmp_path / "churn-example")
    done = _set(
        target,
        "schema_version=1",
        "system={name: Churn next month, slug: churn-example, status: draft, "
        "example: churn-example, target: {cluster: c, project: p, stage: development}}",
        "requirements.status=pending",
        "requirements.system_type=batch",
        "requirements.sla.batch={cadence: daily}",
        "requirements.data_sources+={name: customers, kind: synthetic, shape: batch, "
        "status: needs_generation}",
        "requirements.data_sources+={name: usage_events, kind: synthetic, "
        "shape: events, status: needs_generation}",
        "data.customers.generator.story=5,000 telco customers",
        "data.status=pending",
        "requirements.consumers=ui",
        "app={wanted: true, kind: query_ui, name: churn-example-app, status: pending}",
    )
    assert done.returncode == 0, done.stderr


@pytest.mark.parametrize(
    "example",
    ["churn-example", "recs-example", "helpdesk-example", "gis-example", "run-example"],
)
def test_every_example_creates_a_valid_system(tmp_path, example):
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / example, example)
    done = _set(target, "system.target={cluster: c, project: p, stage: development}")
    assert done.returncode == 0, done.stderr
    # An example built on reference code names a directory the skills ship.
    reference = yaml.safe_load((target / "system.yaml").read_text())[
        "requirements"
    ].get("reference_code")
    if reference:
        prefix = "/opt/hopsworks-api/python/hopsworks/skills/ml/hops-reqs/references/"
        assert (
            reference.startswith(prefix) and (REQS / reference[len(prefix) :]).is_dir()
        )
    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))
    assert doc["system"]["example"] == example
    assert doc["app"]["wanted"] is True


def test_the_builder_runs_on_the_session_model():
    front = _front("hops-build.md")
    text = (TEMPLATES / "hops-build.md").read_text(encoding="utf-8")
    assert "model" not in front
    assert "argument-hint" in front
    for phase in (
        "### reqs",
        "### data",
        "### features",
        "### train",
        "### infer",
        "### app",
        "### verify",
        "### stop",
        "### Finishing",
    ):
        assert phase in text
    for agent in AGENTS:
        assert agent in text
    assert "!`hops fg list" in text and "*/system.yaml 2>/dev/null" in text


def test_every_skill_and_agent_the_templates_name_is_shipped():
    shipped = {p.parent.name for p in SKILLS.glob("*/*/SKILL.md")}
    for template in [*COMMANDS, *(f"{a}.md" for a in AGENTS)]:
        text = (TEMPLATES / template).read_text(encoding="utf-8")
        named = set(re.findall(r"\*\*(hops-[a-z-]+)\*\*", text))
        named |= set(re.findall(r"`(hops-[a-z-]+)/references/", text))
        agents = {n for n in named if n in AGENTS}
        assert all((TEMPLATES / f"{a}.md").is_file() for a in agents), agents
        named -= agents
        # The menu loads no skill; everything that builds does.
        assert named or template == "hops.md", template
        assert named <= shipped, f"{template} names missing skills: {named - shipped}"


def test_the_repository_dev_copies_match_the_templates():
    repo_claude = Path(scaffold.__file__).resolve().parents[3] / ".claude"
    if not repo_claude.is_dir():
        pytest.skip("not running from a source checkout")
    pairs = [(f"commands/{c}", c) for c in COMMANDS]
    pairs += [(f"agents/{a}.md", f"{a}.md") for a in AGENTS]
    for rel, template in pairs:
        assert (repo_claude / rel).read_text(encoding="utf-8") == (
            TEMPLATES / template
        ).read_text(encoding="utf-8")
    assert not (repo_claude / "agents" / "hops-fti.md").exists()


def test_setup_removes_an_unedited_fti_agent_and_keeps_an_edited_one(tmp_path):
    for edited in (False, True):
        root = tmp_path / ("edited" if edited else "unedited")
        old = "old fti agent\n"
        scaffold.scaffold(root, {".claude/agents/hops-fti.md": old})
        if edited:
            (root / ".claude/agents/hops-fti.md").write_text(
                "my notes\n", encoding="utf-8"
            )
        result = scaffold.scaffold(
            root, scaffold.build_files(internal=True, project="p")
        )
        assert (root / ".claude/commands/hops.md").is_file()
        if edited:
            assert result.orphaned == [".claude/agents/hops-fti.md"]
            assert (root / ".claude/agents/hops-fti.md").is_file()
        else:
            assert ".claude/agents/hops-fti.md" in result.removed
            assert not (root / ".claude/agents/hops-fti.md").exists()


# endregion

# region Skills shipping


def test_skills_install_delivers_the_protocol_and_the_template_to_every_agent(tmp_path):
    result = CliRunner().invoke(
        cli,
        [
            "skills",
            "install",
            "--dir",
            str(tmp_path),
            "--agent",
            "claude",
            "--agent",
            "codex",
            "--agent",
            "copilot",
        ],
    )
    assert result.exit_code == 0, result.output
    for agent_dir in (".claude/skills", ".codex/skills", ".agents/skills"):
        base = tmp_path / agent_dir
        assert (base / "hops-train/references/autoresearch.md").is_file()
        assert (base / "hops-reqs/references/system_template/pyproject.toml").is_file()
        assert (base / "hops-reqs/references/system_template/gitignore").is_file()
        assert (base / "hops-reqs/references/example-system.yaml").is_file()
        assert (base / "hops-app/references/app_skeleton/static/app.js").is_file()
        assert (base / "hops-app/references/trino_js/package.json").is_file()
        assert (base / "hops-synthetic-data/SKILL.md").is_file()
        assert not (base / "hops-eda-checklist").exists()


def test_package_data_covers_every_file_the_skills_ship():
    pyproject = Path(scaffold.__file__).resolve().parents[2] / "pyproject.toml"
    if not pyproject.exists():
        pytest.skip("not running from a source checkout")
    block = re.search(
        r'"hopsworks" = \[(.*?)\]', pyproject.read_text(), re.DOTALL
    ).group(1)
    patterns = re.findall(r'"skills/\*\*/([^"]+)"', block)
    for path in SKILLS.rglob("*"):
        rel = path.relative_to(SKILLS).parts
        if path.is_file() and not any(
            p.startswith(".") or p == "__pycache__" for p in rel
        ):
            assert any(path.match(p) for p in patterns), (
                f"{path} is not in package data"
            )


def test_nothing_refers_to_retired_skills_or_agents():
    roots = [SKILLS, TEMPLATES]
    for root in roots:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in (".md", ".py", ".yaml", ".toml"):
                text = path.read_text(encoding="utf-8")
                assert "hops-eda-checklist" not in text, path
                assert "hops-fti" not in text, path


def test_every_skill_is_listed_in_both_readmes():
    top = (SKILLS / "README.md").read_text(encoding="utf-8")
    for skill_md in SKILLS.glob("*/*/SKILL.md"):
        bucket, name = skill_md.parent.parent.name, skill_md.parent.name
        assert f"[{name}]({bucket}/{name}/SKILL.md)" in top, name
        bucket_readme = (skill_md.parent.parent / "README.md").read_text(
            encoding="utf-8"
        )
        assert f"[{name}]({name}/SKILL.md)" in bucket_readme, name


def test_the_data_sources_table_names_every_required_option():
    text = (REQS / "data-sources.md").read_text(encoding="utf-8")
    table = {}
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if (
            len(cells) == 4
            and re.fullmatch(r"[a-z0-9-]+", cells[0])
            and cells[0] != "---"
        ):
            table[cells[0]] = set(re.findall(r"`(--[a-z-]+)`", cells[2]))
    ctx = click.Context(cli)
    create = cli.get_command(ctx, "datasource").get_command(ctx, "create")
    types = create.list_commands(ctx)
    assert set(table) == set(types)
    for name in types:
        command = create.get_command(ctx, name)
        required = {
            max(p.opts, key=len)
            for p in command.params
            if isinstance(p, click.Option) and p.required
        }
        assert table[name] == required, name


# endregion

# region system.yaml: the schema, the validator and the worked example


def test_the_schema_block_parses_as_yaml():
    text = (REQS / "system-yaml.md").read_text(encoding="utf-8")
    schema = re.search(r"## Schema\n\n```yaml\n(.*?)```", text, re.DOTALL).group(1)
    doc = yaml.safe_load(schema)
    for block in (
        "system",
        "requirements",
        "data",
        "features",
        "training",
        "inference",
        "app",
        "verify",
        "decisions",
    ):
        assert block in doc


def test_the_worked_example_is_valid():
    assert _validate(_example()) == []


def _break(path: list, value):
    doc = _example()
    node = doc
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return doc


@pytest.mark.parametrize(
    ("path", "value", "expected"),
    [
        (["schema_version"], 2, "schema_version"),
        (["system", "slug"], "telco_churn", "system.slug"),
        (["system", "target", "stage"], "production", "stage"),
        (["system", "status"], "done", "system.status"),
        (["requirements", "status"], "done", "requirements.status"),
        (["requirements", "sla"], {"batch": {}, "realtime": {}}, "exactly one"),
        (["requirements", "sla"], {"realtime": {"p99_ms": 5}}, "system_type is batch"),
        (["requirements", "problem", "task"], "magic", "task"),
        (["requirements", "targets", "target"], "high", "must be a number"),
        (["requirements", "targets", "direction"], "up", "direction"),
        (
            ["requirements", "operations", "features", "run"],
            "daily",
            "operations.features",
        ),
        (
            ["requirements", "models", "token"],
            "hf_abcdefghijklmnopqrstuvwxyz0123",
            "literal value",
        ),
        (["features", "pipelines", 0, "run"], "hourly", "features.pipelines[0].run"),
        (["features", "pipelines", 0, "job", "name"], "Telco_Features", "job"),
        (["training", "runs", 0, "commit"], None, "has no commit"),
        (["training", "runs", 0, "state"], "done", "state"),
        (["training", "runs", 0, "status"], "maybe", "verdict"),
        (["training", "runs", 0, "model", "name"], "telco_churn_model", "_research"),
        (["training", "model", "name"], "telco_churn_research", "_model"),
        (["inference", "measured", 0, "execution"], None, "no execution"),
        (["decisions", 0, "by"], "robot", "user or claude"),
        (["verify", "status"], "ok", "verify.status"),
        (["training", "estimate"], "soon", "training.estimate"),
    ],
)
def test_the_validator_rejects_each_broken_rule(path, value, expected):
    problems = _validate(_break(path, value))
    assert any(expected in p for p in problems), problems


def _release(version, kind, commit="a1b2c3d"):
    return {"version": version, "tag": f"v{version}", "kind": kind, "commit": commit}


def test_releases_start_at_0_1_0_and_step_by_their_kind():
    validator = _load(TEMPLATE / "tests" / "unit" / "test_system_yaml.py")
    assert validator.next_version(None, "minor") == "0.1.0"
    assert validator.next_version("0.1.0", "patch") == "0.1.1"
    assert validator.next_version("0.1.1", "minor") == "0.2.0"
    assert validator.next_version("0.2.0", "major") == "1.0.0"

    doc = _example()
    doc["system"]["releases"] = [
        _release("0.1.0", "initial"),
        _release("0.1.1", "patch"),
        _release("0.2.0", "minor"),
        _release("1.0.0", "major"),
    ]
    # Changed since the last release: the next version, released when it ships.
    doc["system"]["version"] = "1.0.1"
    assert _validate(doc) == []
    # Unchanged since the last release, the version is that release's.
    doc["system"]["version"] = "1.0.0"
    assert _validate(doc) == []


@pytest.mark.parametrize(
    ("releases", "version", "expected"),
    [
        ([_release("1.0.0", "initial")], "1.0.0", "first release: version 0.1.0"),
        (
            [_release("0.1.0", "initial"), _release("0.3.0", "minor")],
            "0.3.0",
            "so 0.2.0",
        ),
        (
            [_release("0.1.0", "initial"), _release("0.1.1", "bugfix")],
            "0.1.1",
            "kind 'bugfix'",
        ),
        (
            [{**_release("0.1.0", "initial"), "tag": "0.1.0"}],
            "0.1.0",
            "tag must be v0.1.0",
        ),
        ([{**_release("0.1.0", "initial"), "commit": ""}], "0.1.0", "has no commit"),
        ([_release("0.1", "initial")], "0.1.0", "is not MAJOR.MINOR.PATCH"),
        ([], "1.0.0", "must be 0.1.0 until the first release"),
        (
            [_release("0.1.0", "initial")],
            "0.3.0",
            "or the next one: 0.1.1, 0.2.0, 1.0.0",
        ),
        (
            [_release("0.1.0", "initial")],
            "1.0",
            "system.version '1.0' is not MAJOR.MINOR.PATCH",
        ),
        ([_release("0.1.0", "initial")], None, "system.version is missing"),
    ],
)
def test_the_validator_rejects_releases_that_break_the_versioning(
    releases, version, expected
):
    doc = _example()
    doc["system"]["releases"] = releases
    doc["system"].pop("version", None)
    if version:
        doc["system"]["version"] = version
    problems = _validate(doc)
    assert any(expected in problem for problem in problems), problems


def test_new_systems_start_at_0_1_0_and_the_status_says_whether_it_is_released(
    tmp_path,
):
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / "recs-example", "recs-example")
    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))
    assert doc["system"]["version"] == "0.1.0"
    status = _load(TEMPLATE / "status.py", "status_under_test")
    assert status.version_line(doc) == "version 0.1.0, not released yet"
    doc["system"]["releases"] = [
        {
            **_release("0.1.0", "initial"),
            "url": "https://github.com/a/b/releases/tag/v0.1.0",
        }
    ]
    assert status.version_line(doc) == (
        "version 0.1.0, released: https://github.com/a/b/releases/tag/v0.1.0"
    )
    doc["system"]["version"] = "0.2.0"
    assert (
        status.version_line(doc)
        == "version 0.2.0, not released yet (last release 0.1.0)"
    )
    del doc["system"]["version"]
    assert status.version_line(doc) == ""


def test_the_validator_rejects_a_phase_that_finishes_before_it_starts():
    doc = _example()
    doc["training"]["started"] = "2026-09-22T11:00Z"
    doc["training"]["finished"] = "2026-09-22T09:30Z"
    problems = _validate(doc)
    assert any("training.finished" in p and "date -u" in p for p in problems), problems
    doc["training"]["finished"] = "2026-09-22T11:40Z"
    assert not any("training.finished" in p for p in _validate(doc))


def test_the_validator_rejects_a_data_source_without_a_route():
    doc = _example()
    doc["requirements"]["data_sources"][0]["kind"] = "magic"
    doc["requirements"]["data_sources"][2]["shape"] = "blob"
    problems = _validate(doc)
    assert any("one route" in p for p in problems)
    assert any("shape batch or events" in p for p in problems)


def test_the_validator_is_the_gate_for_an_atomic_write(tmp_path):
    good, bad = tmp_path / "good.yaml", tmp_path / "bad.yaml"
    good.write_text(EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")
    bad.write_text(yaml.safe_dump(_break(["schema_version"], 9)), encoding="utf-8")
    script = TEMPLATE / "tests" / "unit" / "test_system_yaml.py"
    assert (
        subprocess.run([sys.executable, str(script), str(good)], check=False).returncode
        == 0
    )
    failed = subprocess.run(
        [sys.executable, str(script), str(bad)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 1
    assert "schema_version" in failed.stderr


def test_a_draft_may_lack_what_the_interview_has_not_asked_yet():
    draft = {
        "schema_version": 1,
        "system": {
            "name": "Churn",
            "slug": "churn",
            "target": {"cluster": "c", "project": "p", "stage": "development"},
            "status": "draft",
        },
        "requirements": {"status": "pending", "description": "who churns"},
    }
    assert _validate(draft) == []
    draft["requirements"]["system_type"] = "nonsense"
    assert any("system_type" in p for p in _validate(draft))
    draft["requirements"].update(system_type="batch", status="met")
    assert any("task" in p for p in _validate(draft))


def _set(target: Path, *assignments: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(target / "set.py"), *assignments],
        capture_output=True,
        text=True,
        check=False,
    )


def test_set_records_interview_answers_and_refuses_an_invalid_one(tmp_path):
    target = _load(REQS / "new_system.py").create(tmp_path / "churn")
    done = _set(
        target,
        "schema_version=1",
        "system={name: Churn, slug: churn, status: draft, "
        "target: {cluster: c, project: p, stage: development}}",
        "requirements.status=pending",
        "requirements.system_type=batch",
        "requirements.sla.batch={cadence: daily}",
        "requirements.data_sources+={name: customers, kind: feature_group, "
        "version: 1, status: present}",
    )
    assert done.returncode == 0, done.stderr
    written = (target / "system.yaml").read_text(encoding="utf-8")
    doc = yaml.safe_load(written)
    assert doc["requirements"]["sla"] == {"batch": {"cadence": "daily"}}
    assert doc["requirements"]["data_sources"][0]["version"] == 1
    refused = _set(target, "requirements.system_type=nonsense")
    assert refused.returncode == 1
    assert "system_type" in refused.stderr
    assert (target / "system.yaml").read_text(encoding="utf-8") == written
    assert not list(target.glob(".system.yaml.*"))


# endregion

# region status.py


def _in_progress() -> dict:
    doc = _example()
    training = doc["training"]
    training["status"] = "running"
    for key in ("finished", "acceptance", "model"):
        training.pop(key)
    training["runs"].append(
        {
            "run_id": "train-4-1",
            "n": 4,
            "attempt": 1,
            "commit": "aa11bb2",
            "state": "running",
            "execution": 1110,
        }
    )
    for key in ("inference", "app", "verify"):
        doc.pop(key)
    doc["system"]["progress"] = {
        "phase": "train",
        "now": "run 4 of 5, execution 1110 at 6m of 10m",
    }
    return doc


def test_status_renders_the_table_the_reference_documents():
    status = _load(TEMPLATE / "status.py")
    rendered = status.render(_in_progress(), status.parse_time("2026-09-22T10:11Z"))
    documented = (REQS / "system-yaml.md").read_text(encoding="utf-8")
    table = re.search(r"```\n(phase .*?)```", documented, re.DOTALL).group(1).strip()
    assert rendered == table


def test_status_of_a_finished_system_has_nothing_left():
    status = _load(TEMPLATE / "status.py")
    rendered = status.render(_example(), status.parse_time("2026-09-22T12:10Z"))
    assert rendered.splitlines()[-1] == "7 of 7 phases done; nothing left to run"
    assert "train      met       09:33    1h32m" in rendered


def test_status_estimates_from_measurements_before_defaults():
    status = _load(TEMPLATE / "status.py")
    now = status.parse_time("2026-09-22T12:00Z")
    doc = _example()
    # A rerun: the phase is pending again but ran before, so its own duration is the estimate.
    doc["inference"]["status"] = "stale"
    rows = {r["phase"]: r for r in status.rows(doc, now)}
    assert rows["infer"]["basis"] == "measured"
    assert status.fmt_duration(rows["infer"]["remaining"]) == "37m"
    # Never run: the default of 3 minutes per attempt.
    doc.pop("inference")
    rows = {r["phase"]: r for r in status.rows(doc, now)}
    assert rows["infer"]["basis"] == "default"
    assert status.fmt_duration(rows["infer"]["remaining"]) == "15m"
    # A training round in progress with no finished run yet: what is left of wall_clock.
    doc = _in_progress()
    doc["training"]["runs"] = []
    rows = {
        r["phase"]: r for r in status.rows(doc, status.parse_time("2026-09-22T09:53Z"))
    }
    assert rows["train"]["basis"] == "budget wall_clock"
    assert status.fmt_duration(rows["train"]["remaining"]) == "40m"
    # The build's own estimate for a phase wins over every default.
    doc = _example()
    doc["inference"] = {"status": "pending", "estimate": "12m"}
    rows = {r["phase"]: r for r in status.rows(doc, now)}
    assert status.fmt_duration(rows["infer"]["remaining"]) == "12m"
    # In progress with no finished run: the estimate less the time spent.
    doc = _in_progress()
    doc["training"]["runs"] = []
    doc["training"]["estimate"] = "30m"
    rows = {
        r["phase"]: r for r in status.rows(doc, status.parse_time("2026-09-22T09:53Z"))
    }
    assert rows["train"]["basis"] == "estimate"
    assert status.fmt_duration(rows["train"]["remaining"]) == "10m"
    # A pretrained model is only downloaded and registered: 5 minutes, not the budget.
    doc = _example()
    doc["training"] = {"required": False, "status": "pending"}
    rows = {r["phase"]: r for r in status.rows(doc, now)}
    assert status.fmt_duration(rows["train"]["remaining"]) == "5m"


def test_status_runs_as_a_script_without_a_session(tmp_path):
    system_dir = tmp_path / "telco-churn"
    system_dir.mkdir()
    (system_dir / "system.yaml").write_text(
        EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8"
    )
    done = subprocess.run(
        [
            sys.executable,
            str(TEMPLATE / "status.py"),
            str(system_dir / "system.yaml"),
            "--now",
            "2026-09-22T12:10Z",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert "nothing left to run" in done.stdout


# endregion

# region The generated system, bundles and the prelude


def test_a_new_system_carries_its_agents_md(tmp_path):
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / "churn")
    agents = (target / "AGENTS.md").read_text(encoding="utf-8")
    assert "built from system.yaml" in agents.splitlines()[0]
    assert "hops fg lineage" in agents and "Brewer-Edit" in agents
    assert not (target / "CLAUDE.md").exists()
    assert not (tmp_path / "AGENTS.md").exists()


def _new_system(tmp_path: Path) -> Path:
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / "telco-churn")
    (target / "system.yaml").write_text(
        EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8"
    )
    return target


def test_new_system_names_the_package_after_the_slug(tmp_path):
    target = _new_system(tmp_path)
    assert (target / "src" / "telco_churn" / "evaluate.py").is_file()
    assert not (target / "src" / "slug_pkg").exists()
    assert (target / ".gitignore").is_file()
    assert (
        "slug_pkg" not in (target / "tests" / "unit" / "test_training.py").read_text()
    )
    new_system = _load(REQS / "new_system.py")
    with pytest.raises(SystemExit):
        new_system.create(target)


def test_a_generated_system_passes_its_own_unit_tests(tmp_path):
    target = _new_system(tmp_path)
    done = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
        cwd=target,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert re.search(r"\b(\d+) passed", done.stdout)


def test_an_integration_run_that_collects_nothing_fails(tmp_path):
    target = _new_system(tmp_path)
    (target / "tests" / "integration" / "test_parity.py").unlink()
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "tests/integration",
        ],
        cwd=target,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 1, done.stdout + done.stderr


def test_every_entrypoint_carries_the_same_prelude():
    def region(path: Path) -> str:
        text = path.read_text(encoding="utf-8")
        start = text.index("# region bundle prelude")
        return text[start : text.index("# endregion", start)]

    first = region(ENTRYPOINTS[0])
    for path in ENTRYPOINTS[1:]:
        assert region(path) == first, path


def _git(target: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@example.com", "-c", "user.name=t", *args],
        cwd=target,
        check=True,
        capture_output=True,
    )


def test_a_bundle_is_checked_against_its_manifest(tmp_path, monkeypatch):
    target = _new_system(tmp_path)
    _git(target, "init", "-q")
    _git(target, "add", "-A")
    _git(target, "commit", "-q", "-m", "system")
    bundle = _load(target / "bundle.py")
    archive = bundle.make("train-1-1", root=target)
    with pytest.raises(SystemExit):
        bundle.make("train-1-1", root=target)

    evaluate = _load(
        target / "src" / "telco_churn" / "evaluate.py", "evaluate_under_test"
    )
    monkeypatch.setattr(sys, "path", list(sys.path))
    workdir, manifest, system = evaluate._load_bundle(str(archive))
    assert manifest["run_id"] == "train-1-1"
    assert manifest["slug"] == "telco-churn"
    assert "src/telco_churn/training_pipeline.py" in manifest["files"]
    assert "tests/unit/test_training.py" not in manifest["files"]
    assert system["system"]["slug"] == "telco-churn"
    assert sys.path[0] == str(workdir / "src")

    monkeypatch.setenv("HOPS_RESULT_DIR", str(tmp_path))
    written = evaluate._write_result(manifest, {"metrics": {"pr_auc": 0.6}})
    result = json.loads(Path(written).read_text())
    assert result["run_id"] == "train-1-1" and result["commit"] == manifest["commit"]

    tampered = tmp_path / "tampered.tar.gz"
    with tarfile.open(archive) as src, tarfile.open(tampered, "w:gz") as dst:
        for member in src.getmembers():
            data = src.extractfile(member).read()
            if member.name == "system.yaml":
                data += b"\n# edited after the bundle was made\n"
                member.size = len(data)
            dst.addfile(member, io.BytesIO(data))
    with pytest.raises(SystemExit, match="does not match its manifest"):
        evaluate._load_bundle(str(tampered))


def test_a_bundle_refuses_uncommitted_changes(tmp_path):
    target = _new_system(tmp_path)
    _git(target, "init", "-q")
    _git(target, "add", "-A")
    _git(target, "commit", "-q", "-m", "system")
    (target / "src" / "telco_churn" / "training_pipeline.py").write_text("# edited\n")
    bundle = _load(target / "bundle.py")
    with pytest.raises(SystemExit, match="uncommitted"):
        bundle.make("train-2-1", root=target)


# endregion

# region The synthetic data generator


def test_the_generator_is_seeded_and_tells_its_story():
    pytest.importorskip("polars")
    generator = _load(GENERATOR, "generator_under_test")
    end = datetime(2026, 9, 22, tzinfo=timezone.utc)
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    entities = generator.entities(2000, 7, 0.26, end)
    assert entities.equals(generator.entities(2000, 7, 0.26, end))
    assert abs(entities["churn"].mean() - 0.26) < 0.01

    events = generator.events(entities, start, end, 0.02, 7)
    assert events.equals(generator.events(entities, start, end, 0.02, 7))
    assert events["event_id"].n_unique() == events.height

    import polars as pl

    late = events.filter(pl.col("ts") > datetime(2026, 8, 23, tzinfo=timezone.utc))
    joined = late.join(entities, on="customer_id")
    churner_share = joined["churn"].mean()
    # Churners are 26% of customers but their calls decay before the end.
    assert churner_share < 0.2


def test_a_live_tick_writes_its_rate_inside_the_tick():
    pytest.importorskip("polars")
    generator = _load(GENERATOR, "generator_under_test")
    now = datetime(2026, 9, 22, 12, 0, 10, tzinfo=timezone.utc)
    entities = generator.entities(100, 7, 0.2, now)
    tick = generator.tick(entities, now, 10, 5, 7)
    assert tick.height == 50
    assert tick["ts"].min() >= datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)
    assert tick["ts"].max() < now
    assert tick.equals(generator.tick(entities, now, 10, 5, 7))


def test_an_offline_source_gets_an_offline_delta_group():
    pytest.importorskip("polars")
    generator = _load(GENERATOR, "generator_under_test")

    class FakeStore:
        def get_or_create_feature_group(self, **kwargs):
            self.kwargs = kwargs

    fs = FakeStore()
    generator._sink(fs, {"feature_group": "usage_events", "online": False})
    assert fs.kwargs["online_enabled"] is False
    assert fs.kwargs["time_travel_format"] == "DELTA"
    assert "stream" not in fs.kwargs
    assert "ttl" not in fs.kwargs
    generator._sink(fs, {"feature_group": "usage_events"})
    assert fs.kwargs["online_enabled"] is True
    assert fs.kwargs["stream"] is True


# endregion

# region The recommender


RECS = REQS / "recommender"


def test_the_hm_features_keep_the_ids_the_pictures_and_the_month_cycle():
    pl = pytest.importorskip("polars")
    features = _load(RECS / "hm_features.py", "hm_features_under_test")
    articles = features.compute_articles(
        pl.DataFrame(
            {
                **{c: ["x"] for c in features.ARTICLE_COLUMNS},
                "article_id": ["0108775015"],
            }
        ).with_columns(detail_desc=pl.lit(None, pl.Utf8))
    )
    assert articles["article_id"][0] == "108775015"
    assert articles["image_url"][0].endswith("/images/010/0108775015.jpg")
    transactions = features.compute_transactions(
        pl.DataFrame(
            {
                "t_dat": [date(2020, 3, 1), date(2020, 3, 1), date(2020, 9, 1)],
                "customer_id": ["c", "c", "c"],
                "article_id": [108775015, 108775015, 108775016],
                "price": [0.1, 0.1, 0.2],
                "sales_channel_id": [2, 2, 1],
            }
        )
    )
    assert transactions.height == 2
    assert transactions["article_id"].to_list() == ["108775015", "108775016"]
    assert transactions["month_sin"][0] == pytest.approx(1.0)


def test_the_hm_features_read_only_the_sampled_customers_purchases(tmp_path):
    pytest.importorskip("polars")
    features = _load(RECS / "hm_features.py", "hm_features_under_test")
    a, b = "a" * 64, "b" * 64
    csv = tmp_path / "transactions_train.csv"
    csv.write_text(
        "t_dat,customer_id,article_id,price,sales_channel_id\n"
        f"2018-09-20,{a},0663713001,0.05,2\n"
        f"2018-09-20,{b},0541518023,0.03,2\n"
        f"2018-09-21,{a},0505221004,0.01,1\n"
    )
    kept = features.read_purchases(csv.as_uri(), [a])
    assert kept["customer_id"].to_list() == [a, a]
    assert kept["article_id"].to_list() == [663713001, 505221004]


def test_synthetic_purchases_follow_each_customers_own_groups():
    pl = pytest.importorskip("polars")
    features = _load(RECS / "hm_features.py", "hm_features_under_test")
    articles = pl.DataFrame(
        {
            "article_id": ["1", "2", "3", "4"],
            "index_group_name": ["Ladies", "Ladies", "Men", "Men"],
            "garment_group_name": ["Dress", "Dress", "Shoes", "Shoes"],
        }
    )
    transactions = pl.DataFrame(
        {
            "t_dat": [
                datetime(2020, 3, 1),
                datetime(2020, 3, 11),
                datetime(2020, 5, 1),
            ],
            "customer_id": ["c", "c", "d"],
            "article_id": ["1", "1", "3"],
            "price": [0.1, 0.1, 0.2],
            "sales_channel_id": [2, 2, 1],
        }
    )
    extra = features.synthetic_purchases(transactions, articles, 25, 7)
    per_customer = dict(extra.group_by("customer_id").len().iter_rows())
    # Draws that land on the same article and day are one purchase: d has one of each.
    assert per_customer["d"] == 1
    assert 1 < per_customer["c"] <= 25
    assert set(extra.filter(pl.col("customer_id") == "c")["article_id"]) <= {"1", "2"}
    assert set(extra.filter(pl.col("customer_id") == "d")["article_id"]) == {"3"}
    c_days = extra.filter(pl.col("customer_id") == "c")["t_dat"]
    assert c_days.min() >= datetime(2020, 3, 1) and c_days.max() <= datetime(
        2020, 3, 11
    )
    assert extra.columns == transactions.columns + ["month_sin", "month_cos"]


def test_the_generated_interactions_surround_every_purchase():
    pl = pytest.importorskip("polars")
    features = _load(RECS / "hm_features.py", "hm_features_under_test")
    transactions = pl.DataFrame(
        {
            "t_dat": [datetime(2020, 3, d) for d in (1, 5, 9)],
            "customer_id": ["c", "c", "d"],
            "article_id": ["1", "2", "3"],
        }
    )
    interactions = features.generate_interactions(
        transactions, pl.Series(["1", "2", "3", "4"]), 7
    )
    purchases = interactions.filter(pl.col("interaction_score") == 2)
    assert purchases.height == 3
    per_customer = (
        interactions.group_by("customer_id").len().sort("customer_id")["len"].to_list()
    )
    assert all(n >= 3 + 40 for n in per_customer[:1])
    first = interactions.filter(pl.col("customer_id") == "c").sort("t_dat")
    assert first["prev_article_id"][0] == "START"
    assert first["prev_article_id"][1:].to_list() == first["article_id"][:-1].to_list()
    assert interactions.equals(
        features.generate_interactions(transactions, pl.Series(["1", "2", "3", "4"]), 7)
    )


def test_retrieval_recall_counts_the_true_article_in_the_top_k_chunk_by_chunk():
    retrieval = _load(RECS / "train_retrieval.py", "train_retrieval_under_test")
    rng = np.random.default_rng(0)
    query, items = rng.normal(size=(300, 4)), rng.normal(size=(50, 4))
    true_items = rng.integers(-1, 50, 300)
    top = np.argsort(-(query @ items.T), axis=1)[:, :10]
    exact = np.mean([t in row for t, row in zip(true_items, top, strict=True)])
    assert retrieval.recall_at_k(query, items, true_items, k=10) == pytest.approx(exact)
    assert retrieval.recall_at_k(
        query, items, true_items, k=10, chunk=7
    ) == pytest.approx(exact)


def test_the_ranker_learns_from_earlier_purchases_and_labels_the_latest():
    pl = pytest.importorskip("polars")
    ranker = _load(RECS / "train_ranker.py", "train_ranker_under_test")
    days = [datetime(2020, 1, d) for d in (1, 2, 3, 4, 5)]
    purchases = pl.DataFrame(
        {
            "customer_id": ["c"] * 5 + ["d"],
            "article_id": ["1", "1", "2", "2", "3", "4"],
            "t_dat": [*days, days[0]],
        }
    )
    history, labels = ranker.split_history(purchases)
    # d has one purchase and nothing to learn from; c's latest fifth is the label.
    assert labels["article_id"].to_list() == ["3"]
    assert history["article_id"].to_list() == ["1", "1", "2", "2"]

    customers = pl.DataFrame({"customer_id": ["c", "d"], "age": [30.0, 40.0]})
    colours = {"1": "Black", "2": "Red", "3": "Red", "4": "Black"}
    articles = pl.DataFrame(
        {
            "article_id": list(colours),
            **{c: ["x"] * 4 for c in ranker.CATEGORICAL},
        }
    ).with_columns(colour_group_name=pl.Series(list(colours.values())))
    pairs = ranker.ranking_pairs(purchases, customers, articles)
    assert pairs.columns == [*ranker.FEATURES, "label"]
    positive = pairs.filter(pl.col("label") == 1)
    assert positive.height == 1
    # Article 3 is red and half of c's earlier purchases were red; the label never
    # counts towards its own share.
    assert positive["colour_group_name_share"][0] == pytest.approx(0.5)
    assert positive["index_group_name_share"][0] == pytest.approx(1.0)
    assert ranker.roc_auc(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9])) == 1.0
    assert ranker.roc_auc(np.array([0, 1]), np.array([0.5, 0.5])) == 0.5
    metrics = ranker.evaluate(np.array([0, 1, 1]), np.array([0.2, 0.9, 0.4]))
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 0.5
    assert all(type(v) is float for v in metrics.values())


def test_the_deployment_ranks_by_taste_what_the_customer_has_not_bought():
    pd = pytest.importorskip("pandas")
    predictor = _load(RECS / "predictor.py", "recs_predictor_under_test")

    class Model:
        def predict_proba(self, rows, thread_count=-1):
            score = 0.2 + 0.7 * rows["colour_group_name_share"]
            return np.column_stack([1 - score, score])

    articles = pd.DataFrame(
        {
            "article_id": ["1", "2", "3", None],
            "prod_name": ["a", "b", "c", None],
            "colour_group_name": ["blue", "red", "red", None],
            "image_url": ["u1", None, "u3", None],
        }
    )
    history = pd.DataFrame(
        {
            "article_id": ["3", "9", "8", "7"],
            "colour_group_name": ["red", "red", "red", "blue"],
        }
    )
    spec = {
        "features": ["age", "colour_group_name", "colour_group_name_share"],
        "categorical": ["colour_group_name"],
        "taste": ["colour_group_name"],
    }
    items = predictor.rank(
        ["1", "2", "3", "4", "2"], history, articles, 30.0, Model(), spec, 5
    )
    assert [i["article_id"] for i in items] == ["2", "1"]
    assert items[0]["score"] == pytest.approx(0.2 + 0.7 * 0.75)
    assert items[1]["score"] == pytest.approx(0.2 + 0.7 * 0.25)
    assert items[0]["image_url"] is None
    no_history = predictor.rank(
        ["1"], history.iloc[0:0], articles, 30.0, Model(), spec, 5
    )
    assert no_history[0]["score"] == pytest.approx(0.2)
    sin, cos = predictor.month_cycle(datetime(2026, 3, 1, tzinfo=timezone.utc))
    assert sin == pytest.approx(1.0)
    assert cos == pytest.approx(0.0, abs=1e-9)


def test_session_embeddings_are_centered_and_unit_length():
    retrieval = _load(RECS / "train_retrieval.py", "train_retrieval_under_test")
    # Three articles sharing one large direction, as trained item embeddings do.
    shared = np.array([30.0, 30.0])
    vectors = np.stack([shared + [1, 0], shared + [0.9, 0.1], shared + [-1, 0]])
    raw = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    assert raw[0] @ raw[2] > 0.99
    session = retrieval.session_embeddings(vectors)
    assert np.linalg.norm(session, axis=1) == pytest.approx(np.ones(3))
    assert session[0] @ session[1] > 0.9
    assert session[0] @ session[2] < -0.9


def test_the_session_steers_retrieval_and_ranking_and_leaves_room_to_explore():
    pytest.importorskip("pandas")
    predictor = _load(RECS / "predictor.py", "recs_predictor_under_test")
    assert predictor.session_vector(np.zeros((0, 2))) is None
    # Newest first, each older article counting 0.7 of the next.
    newest, older = np.array([1.0, 0.0]), np.array([0.0, 1.0])
    session = predictor.session_vector(np.stack([newest, older]))
    assert session == pytest.approx(np.array([1, 0.7]) / 1.7)

    query = np.array([0.0, 3.0])
    assert predictor.blend(query, None, 0.6) is query
    shoe = np.array([1.0, 0.0])
    turned = predictor.blend(query, shoe, 0.6)
    assert np.linalg.norm(turned) == pytest.approx(3.0)
    assert turned[0] > 0 and turned[1] > 0

    def items():
        return [
            {"article_id": str(i), "score": p}
            for i, p in enumerate([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05])
        ]

    rng = np.random.default_rng(0)
    plain = predictor.select(items(), {}, None, 5, rng)
    assert [i["article_id"] for i in plain[:4]] == ["0", "1", "2", "3"]
    assert [i["reason"] for i in plain] == ["taste"] * 4 + ["explore"]
    assert plain[4]["article_id"] in {"4", "5", "6", "7", "8", "9"}

    # Articles 8 and 9 rank last by purchase probability but are the shoes the
    # session is about: they take half of the slots not left to exploring.
    embeddings = {str(i): np.array([0.0, 1.0]) for i in range(10)}
    embeddings |= {"8": shoe, "9": np.array([0.9, 0.1])}
    steered = predictor.select(items(), embeddings, shoe, 8, rng)
    reasons = [i["reason"] for i in steered]
    assert reasons == ["session"] * 3 + ["taste"] * 3 + ["explore"] * 2
    assert [i["article_id"] for i in steered[:2]] == ["8", "9"]
    assert steered[0]["session_similarity"] == pytest.approx(1.0)
    assert len({i["article_id"] for i in steered}) == 8
    # Nothing is left to explore when every candidate is shown.
    assert "explore" not in {
        i["reason"] for i in predictor.select(items(), embeddings, shoe, 10, rng)
    }
    assert len(predictor.select(items()[:3], {}, None, 5, rng)) == 3


def test_the_storefront_records_clicks_purchases_and_ignores(monkeypatch):
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    storefront = _load(RECS / "app" / "app.py", "recs_app_under_test")
    written = []
    monkeypatch.setattr(
        storefront, "_insert", lambda name, rows: written.append((name, rows))
    )
    client = TestClient(storefront.app)
    assert client.get("/health").json() == {"status": "ok"}
    assert "Storefront" in client.get("/").text
    assert client.get("/static/app.js").status_code == 200

    reply = client.post(
        "/api/interactions",
        json={
            "customer_id": "c",
            "kind": "ignore",
            "article_ids": ["1", "2"],
            "prev_article_id": "9",
        },
    )
    assert reply.json() == {"recorded": 2}
    name, rows = written.pop()
    assert name == "interactions"
    assert [r["interaction_score"] for r in rows] == [0, 0]
    assert [r["prev_article_id"] for r in rows] == ["9", "1"]
    assert rows[0]["t_dat"] != rows[1]["t_dat"]

    client.post(
        "/api/interactions",
        json={"customer_id": "c", "kind": "buy", "article_ids": ["5"]},
    )
    assert [name for name, _ in written] == ["interactions", "transactions"]
    purchase = written[1][1][0]
    assert purchase["article_id"] == "5"
    assert written[0][1][0]["interaction_score"] == 2
    assert (
        client.post(
            "/api/interactions",
            json={"customer_id": "c", "kind": "stare", "article_ids": ["5"]},
        ).status_code
        == 422
    )

    class Deployment:
        def predict(self, data):
            assert data == {"instances": [{"customer_id": "c", "k": 12, "recent": []}]}
            return {
                "predictions": [
                    {"items": [], "timings_ms": {"query": 2.0, "rank": 9.5}}
                ]
            }

    monkeypatch.setattr(storefront, "_deployment", Deployment)
    reply = client.post("/api/recommend", json={"customer_id": "c"}).json()
    assert reply["timings_ms"] == {"query": 2.0, "rank": 9.5}
    assert reply["round_trip_ms"] >= 0


# endregion

# region The dashboard program


class FakeSuperset:
    """The subset of the Superset API the dashboard program uses, held in memory."""

    def __init__(self):
        self.next_id = 100
        self.databases = [{"id": 3, "database_name": "Trino"}]
        self.datasets, self.charts, self.dashboards = {}, {}, {}

    def _id(self) -> int:
        self.next_id += 1
        return self.next_id

    def _request(self, method, path):
        resource, page = re.match(r"/api/v1/(\w+)/\?q=\(page:(\d+)", path).groups()
        store = {
            "dataset": self.datasets,
            "chart": self.charts,
            "dashboard": self.dashboards,
        }[resource]
        items = list(store.values())[int(page) * 100 : (int(page) + 1) * 100]
        return {"result": [dict(i) for i in items]}

    def list_databases(self):
        return {"result": self.databases}

    def create_dataset(self, database_id, table_name, schema, sql):
        i = self._id()
        self.datasets[i] = {
            "id": i,
            "table_name": table_name,
            "schema": schema,
            "sql": sql,
        }
        return {"id": i}

    def delete_dataset(self, dataset_id):
        del self.datasets[dataset_id]

    def create_chart(self, slice_name, viz_type, datasource_id, params):
        i = self._id()
        self.charts[i] = {
            "id": i,
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": datasource_id,
            "params": params,
            "dashboards": [],
        }
        return {"id": i}

    def get_chart(self, chart_id):
        chart = self.charts[chart_id]
        return {
            "result": {**chart, "dashboards": [{"id": d} for d in chart["dashboards"]]}
        }

    def update_chart(self, chart_id, dashboards):
        self.charts[chart_id]["dashboards"] = list(dashboards)

    def delete_chart(self, chart_id):
        del self.charts[chart_id]

    def create_dashboard(self, dashboard_title, published, position_json):
        i = self._id()
        self.dashboards[i] = {
            "id": i,
            "dashboard_title": dashboard_title,
            "position_json": position_json,
        }
        return {"id": i}

    def update_dashboard(self, dashboard_id, **fields):
        self.dashboards[dashboard_id].update(fields)

    def delete_dashboard(self, dashboard_id):
        del self.dashboards[dashboard_id]


def _dashboard_program():
    return _load(DASHBOARD, "dashboard_program_under_test")


def test_the_dashboard_program_creates_then_updates_in_place():
    program, api = _dashboard_program(), FakeSuperset()
    first = program.build(api, "SkillsTest")
    second = program.build(api, "SkillsTest")
    assert first == second
    assert len(api.dashboards) == 1
    assert len(api.charts) == len(program.CHARTS)
    assert len(api.datasets) == 1
    dataset = next(iter(api.datasets.values()))
    assert dataset["sql"] == "SELECT * FROM delta.skillstest_featurestore.customers_1"
    assert all(c["dashboards"] == [first] for c in api.charts.values())
    layout = json.loads(api.dashboards[first]["position_json"])
    for row in layout["GRID_ID"]["children"]:
        assert sum(layout[c]["meta"]["width"] for c in layout[row]["children"]) <= 12


def test_an_edit_that_drops_a_chart_leaves_no_trace_of_it():
    program, api = _dashboard_program(), FakeSuperset()
    program.build(api, "p")
    program.CHARTS = program.CHARTS[:2]
    program.build(api, "p")
    assert sorted(c["slice_name"] for c in api.charts.values()) == sorted(
        c["slice_name"] for c in program.CHARTS
    )


def test_delete_removes_only_what_the_program_made():
    program, api = _dashboard_program(), FakeSuperset()
    other_dashboard = api.create_dashboard("Someone else's", True, "{}")["id"]
    shared = api.create_dataset(3, "orders_1", "p_featurestore", "SELECT 1")["id"]
    foreign = api.create_chart("Total Customers", "big_number_total", shared, "{}")[
        "id"
    ]
    api.update_chart(foreign, dashboards=[other_dashboard])

    program.build(api, "p")
    assert foreign in api.charts
    removed = program.delete(api, "p")
    assert removed["dashboards"] and removed["charts"] and removed["datasets"]
    assert set(api.dashboards) == {other_dashboard}
    assert set(api.charts) == {foreign}
    assert set(api.datasets) == {shared}


def test_delete_keeps_a_dataset_another_chart_still_reads():
    program, api = _dashboard_program(), FakeSuperset()
    program.build(api, "p")
    dataset = next(iter(api.datasets))
    reader = api.create_chart("Someone's chart", "table", dataset, "{}")["id"]
    api.update_chart(
        reader, dashboards=[api.create_dashboard("Other", True, "{}")["id"]]
    )
    program.delete(api, "p")
    assert dataset in api.datasets


# endregion

# region The app skeleton


def test_the_app_skeleton_serves_health_the_page_and_its_assets():
    pytest.importorskip("fastapi")
    from starlette.testclient import TestClient

    app = _load(APP / "app.py", "app_skeleton_under_test")
    client = TestClient(app.app)
    assert client.get("/health").json() == {"status": "ok"}
    page = client.get("/")
    assert page.status_code == 200 and "static/app.js" in page.text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/app.css").status_code == 200


def test_the_app_skeleton_uses_only_relative_urls():
    script = (APP / "static" / "app.js").read_text(encoding="utf-8")
    page = (APP / "static" / "index.html").read_text(encoding="utf-8")
    assert re.findall(r"getJSON\(\s*[`\"']([^`\"']+)", script)
    for url in re.findall(r"getJSON\(\s*[`\"']([^`\"']+)", script):
        assert not url.startswith(("/", "http")), url
    for url in re.findall(r'(?:src|href)="([^"]+)"', page):
        assert not url.startswith(("/", "http")), url
    assert "cdn" not in page.lower() and "https://" not in page


def test_trino_env_exports_the_connection_for_eval(tmp_path, monkeypatch):
    ca = tmp_path / "ca_chain.pem"
    ca.write_text("CA", encoding="utf-8")
    script = (TRINO_JS / "trino_env.py").read_text(encoding="utf-8")
    (tmp_path / "trino_env.py").write_text(
        script.replace("/tmp/ca_chain.pem", str(ca)), encoding="utf-8"
    )
    # The login banner must not reach stdout, which the entrypoint evals.
    (tmp_path / "hopsworks.py").write_text(
        textwrap.dedent(
            """
            class _Trino:
                def get_basic_auth(self):
                    return "Demo__meb10000", "p'w $(x) \\\\"
                def get_host(self):
                    return "coordinator.trino.service.consul"
                def get_port(self):
                    return 8443

            class _Project:
                name = "Demo"
                def get_trino_api(self):
                    return _Trino()

            def login():
                print("Logged in to project Demo")
                return _Project()
            """
        ),
        encoding="utf-8",
    )
    exports = subprocess.run(
        [sys.executable, "trino_env.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
    ).stdout
    shown = subprocess.run(
        [
            "bash",
            "-c",
            'eval "$1"; printf "%s\\n" "$TRINO_SERVER" "$TRINO_USER" "$TRINO_PASSWORD" "$TRINO_CA" "$TRINO_SCHEMA"',
            "_",
            exports,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert shown == [
        "https://coordinator.trino.service.consul:8443",
        "Demo__meb10000",
        "p'w $(x) \\",
        str(ca),
        "demo_featurestore",
    ]


def test_the_trino_js_app_queries_on_the_server_and_uses_relative_urls():
    server = (TRINO_JS / "server.js").read_text(encoding="utf-8")
    script = (TRINO_JS / "static" / "app.js").read_text(encoding="utf-8")
    page = (TRINO_JS / "static" / "index.html").read_text(encoding="utf-8")
    assert '"/health"' in server and "ssl: { ca:" in server
    assert "trino-client" not in script and "service.consul" not in script
    for url in re.findall(r"fetch\(\s*[`\"']([^`\"']+)", script):
        assert not url.startswith(("/", "http")), url
    for url in re.findall(r'(?:src|href)="([^"]+)"', page):
        assert not url.startswith(("/", "http")), url


# endregion


# region Lint


def _lint(target):
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests/unit/test_lint.py"],
        cwd=target,
        capture_output=True,
        text=True,
        check=False,
    )


def test_a_new_system_passes_its_own_lint_test(tmp_path):
    pytest.importorskip("ruff")
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / "telco-churn")
    done = _lint(target)
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_files_a_build_copies_in_pass_the_systems_rules(tmp_path):
    """The generator, the reference examples and the app skeleton become the system's code."""
    pytest.importorskip("ruff")
    new_system = _load(REQS / "new_system.py")
    target = new_system.create(tmp_path / "helpdesk-example")
    package = target / "src" / "helpdesk_example"
    shutil.copy(
        SKILLS / "data" / "hops-synthetic-data" / "references" / "generator.py",
        package / "synthetic_data.py",
    )
    for name in ("agent.py", "ingest_docs.py", "register_embedder.py"):
        shutil.copy(REQS / "rag_agent" / name, package / name)
    shutil.copytree(REQS / "rag_agent" / "app", target / "app")
    shutil.copytree(APP, target / "app-skeleton")
    for name in (
        "hm_features.py",
        "train_retrieval.py",
        "train_ranker.py",
        "predictor.py",
    ):
        shutil.copy(REQS / "recommender" / name, package / f"recs_{name}")
    shutil.copytree(REQS / "recommender" / "app", target / "recs-app")
    shutil.copy(REQS / "kumo_run" / "register_kumo.py", package / "register_kumo.py")
    shutil.copy(REQS / "kumo_run" / "predictor.py", package / "kumo_predictor.py")
    shutil.copytree(REQS / "kumo_run" / "app", target / "run-app")
    done = _lint(target)
    assert done.returncode == 0, done.stdout + done.stderr


# endregion


# region Hops Run with Kumo Tabular


KUMO = REQS / "kumo_run"


def _kumo_rules(monkeypatch):
    monkeypatch.syspath_prepend(str(KUMO / "app"))
    return _load(KUMO / "app" / "game_rules.py", "game_rules")


def test_the_rules_label_every_situation_with_a_move_the_hops_can_make(monkeypatch):
    rules = _kumo_rules(monkeypatch)
    every = rules.situations()
    # Three lanes, each with an empty row or one or two of the three obstacle kinds.
    assert len(every) == 3 * (1 + 3 * 3 + 3 * 9)
    for lane, near in every:
        move = rules.rule_move(lane, near)
        assert not (move == "left" and lane == "left")
        assert not (move == "right" and lane == "right")
    assert rules.rule_move("centre", {"centre": "wall", "left": "wall"}) == "right"
    assert rules.rule_move("left", {"left": "low"}) == "up"
    assert rules.rule_move("right", {"right": "bar"}) == "down"
    assert rules.rule_move("centre", {"left": "wall"}) == "hold"

    context, held_out = rules.split()
    assert len(context) + len(held_out) == len(every)
    assert len(held_out) == round(len(every) * 0.3)
    seen = {tuple(sorted(r.items())) for r in context}
    for lane, near in held_out:
        assert (
            tuple(
                sorted(
                    {
                        **rules.features(lane, near),
                        "move": rules.rule_move(lane, near),
                    }.items()
                )
            )
            not in seen
        )
    assert set(context[0]) == {"lane", "left_lane", "centre_lane", "right_lane", "move"}

    row, allowed = rules.query(
        {
            "lane": "left",
            "airborne": True,
            "ahead": [{"distance": 30, "lanes": {"left": "wall"}}],
        }
    )
    assert row == {
        "lane": "left",
        "left_lane": "wall",
        "centre_lane": "open",
        "right_lane": "open",
    }
    assert allowed == ["hold", "right", "down"]
    with pytest.raises(ValueError):
        rules.query({"lane": "up"})


def test_the_game_asks_kumo_and_keeps_one_board(monkeypatch, tmp_path):
    from starlette.testclient import TestClient

    _kumo_rules(monkeypatch)
    monkeypatch.setenv("BOARD_FILE", str(tmp_path / "board.json"))
    game = _load(KUMO / "app" / "app.py", "kumo_app_under_test")
    asked = []

    def classify(rows):
        asked.append(rows)
        return [
            {"probabilities": {"left": 0.5, "right": 0.2, "hold": 0.3}, "seconds": 0.4}
        ]

    monkeypatch.setattr(game, "classify", classify)
    monkeypatch.setattr(game, "_kumo", lambda: {"model": "kumo_tabular v1"})
    client = TestClient(game.app)
    page = client.get("/").text
    assert "No runs yet" in page and "{{" not in page and "static/pilot.js" in page

    # In the left lane the hops cannot go left: that probability is masked and the rest renormalised.
    decision = client.post(
        "/api/decide", json={"lane": "left", "airborne": False, "ahead": []}
    ).json()
    assert asked == [
        [
            {
                "lane": "left",
                "left_lane": "open",
                "centre_lane": "open",
                "right_lane": "open",
            }
        ]
    ]
    assert decision["moves"] == ["hold", "right", "up", "down"]
    assert decision["probabilities"] == pytest.approx([0.6, 0.4, 0.0, 0.0])
    assert decision["pilot"] == "kumo" and "kumo_tabular v1" in decision["model"]

    # A player's run needs a takeoff key and the time to have flown it.
    key = client.post("/api/runs/start").json()["runKey"]
    too_far = client.post(
        "/api/runs",
        json={"name": "jim", "distance": 5000, "durationMs": 1000, "runKey": key},
    )
    assert too_far.status_code == 400
    unknown = client.post(
        "/api/runs",
        json={
            "name": "jim",
            "distance": 10,
            "durationMs": 500,
            "runKey": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert unknown.status_code == 400
    monkeypatch.setitem(game.started_runs, key, game.started_runs[key] - 10)
    assert (
        client.post(
            "/api/runs",
            json={"name": "jim", "distance": 300, "durationMs": 5000, "runKey": key},
        ).json()["rank"]
        == 1
    )

    pilot = client.post(
        "/api/runs",
        json={
            "name": "Kumo Tabular",
            "pilot": "kumo",
            "distance": 200,
            "durationMs": 4000,
            "runKey": str(uuid.uuid4()),
        },
    ).json()
    assert (pilot["number"], pilot["best"]) == (1, 200)
    assert [r["pilot"] for r in pilot["runs"]] == ["player", "kumo"]
    # The board is kept: a new process reads it back.
    assert len(game.Board(tmp_path / "board.json").runs) == 2


def test_kumo_is_registered_once_per_revision():
    registration = _load(KUMO / "register_kumo.py", "register_kumo_under_test")
    assert registration.FILES[0] == "small/classifier.pt"

    class Registry:
        def __init__(self, descriptions):
            self.models = [type("M", (), {"description": d})() for d in descriptions]

        def get_models(self, name):
            return self.models

    revision = registration.REVISION
    assert registration.already_registered(
        Registry([f"NVIDIA Kumo Tabular (small) ..., nvidia/Kumo-Tabular@{revision}"]),
        "m",
        revision,
    )
    # The medium checkpoint at the same revision is another model.
    assert not registration.already_registered(
        Registry([f"NVIDIA Kumo Tabular (medium) ..., nvidia/Kumo-Tabular@{revision}"]),
        "m",
        revision,
    )
    assert not registration.already_registered(
        Registry(["(small) @other"]), "m", revision
    )


# endregion
