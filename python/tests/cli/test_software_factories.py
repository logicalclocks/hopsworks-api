"""The /hops software factories: the command, the agents, and the files the skills ship.

The skills carry programs that end up running in users' ML systems (the system
template, the synthetic data generator, the dashboard program, the app
skeleton), so they are tested here like code, offline.
"""

from __future__ import annotations

import importlib.util
import io
import json
import re
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

import click
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
    spec.loader.exec_module(module)
    return module


def _example() -> dict:
    return yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))


def _validate(doc: dict) -> list[str]:
    return _load(TEMPLATE / "tests" / "unit" / "test_system_yaml.py").validate(doc)


# region The command and the agents


def test_the_cli_bundle_ships_the_command_and_both_agents_and_retires_fti():
    files = scaffold.build_files(internal=True, project="p")
    assert ".claude/commands/hops.md" in files
    assert ".claude/agents/hops-train-agent.md" in files
    assert ".claude/agents/hops-infer-agent.md" in files
    assert not any("hops-fti" in path for path in files)
    assert not (TEMPLATES / "hops-fti.md").exists()


@pytest.mark.parametrize("name", ["hops-train-agent", "hops-infer-agent"])
def test_agent_definitions_use_the_subagent_frontmatter(name):
    text = (TEMPLATES / f"{name}.md").read_text(encoding="utf-8")
    front = yaml.safe_load(text.split("---")[1])
    assert front["name"] == name
    assert front["description"]
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
    assert "model" not in front
    assert "never asks the user" in front["description"]


def test_the_command_dispatches_every_factory_and_verb():
    text = (TEMPLATES / "hops.md").read_text(encoding="utf-8")
    front = yaml.safe_load(text.split("---")[1])
    assert "argument-hint" in front
    for heading in (
        "## /hops ml",
        "## /hops dashboard",
        "## /hops app",
        "## /hops status",
    ):
        assert heading in text
    for verb in ("`status`", "`verify`", "`stop`", "`dashboard`", "`app`", "`ml`"):
        assert verb in text
    assert "hops-train-agent" in text and "hops-infer-agent" in text
    assert "AskUserQuestion" in text


def test_every_skill_the_templates_name_is_shipped():
    shipped = {p.parent.name for p in SKILLS.glob("*/*/SKILL.md")}
    for template in ("hops.md", "hops-train-agent.md", "hops-infer-agent.md"):
        text = (TEMPLATES / template).read_text(encoding="utf-8")
        named = set(re.findall(r"\*\*(hops-[a-z-]+)\*\*", text))
        named |= set(re.findall(r"`(hops-[a-z-]+)/references/", text))
        agents = {n for n in named if n.endswith("-agent")}
        assert all((TEMPLATES / f"{a}.md").is_file() for a in agents), agents
        named -= agents
        assert named, template
        assert named <= shipped, f"{template} names missing skills: {named - shipped}"


def test_the_repository_dev_copies_match_the_templates():
    repo_claude = Path(scaffold.__file__).resolve().parents[3] / ".claude"
    if not repo_claude.is_dir():
        pytest.skip("not running from a source checkout")
    for rel, template in (
        ("commands/hops.md", "hops.md"),
        ("agents/hops-train-agent.md", "hops-train-agent.md"),
        ("agents/hops-infer-agent.md", "hops-infer-agent.md"),
    ):
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
    ],
)
def test_the_validator_rejects_each_broken_rule(path, value, expected):
    problems = _validate(_break(path, value))
    assert any(expected in p for p in problems), problems


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


# endregion
