"""`hops build`: the structured interview that writes system.yaml, then starts the build."""

from __future__ import annotations

import io
import json
import sys
import threading
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner
from hopsworks.cli import auth
from hopsworks.cli.commands import build
from hopsworks.cli.main import cli


if TYPE_CHECKING:
    from pathlib import Path


yaml = pytest.importorskip("yaml")


@pytest.fixture
def quiet(monkeypatch):
    """No login, no project listing, no model call, no Claude Code."""
    monkeypatch.setattr(build._Prefetch, "run", lambda self: None)
    monkeypatch.setattr(build.shutil, "which", lambda name: None)
    monkeypatch.delenv("TMUX", raising=False)


def _run(tmp_path: Path, monkeypatch, answers: list[str], *args: str):
    monkeypatch.chdir(tmp_path)
    return CliRunner().invoke(
        cli, ["build", "--no-launch", *args], input="\n".join(answers) + "\n"
    )


def _doc(target: Path) -> dict:
    return yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))


def test_an_example_asks_only_where_the_code_goes(tmp_path, monkeypatch, quiet):
    done = _run(tmp_path, monkeypatch, ["2", "1", "1"])
    assert done.exit_code == 0, done.output
    doc = _doc(tmp_path / "churn-example")
    assert doc["system"]["example"] == "churn-example"
    assert doc["app"]["wanted"] is True
    assert {s["kind"] for s in doc["requirements"]["data_sources"]} == {"synthetic"}
    assert doc["system"]["repo"] == {"url": "new"}
    assert (
        "Which data" not in done.output and "How are the predictions" not in done.output
    )
    assert 'claude "/hops-build churn-example"' in done.output


def test_a_described_batch_system_records_every_answer(tmp_path, monkeypatch, quiet):
    advice = {
        "system_type": "batch",
        "reason": "the list is read once a day",
        "name": "Churn next month",
        "slug": "churn",
        "feature_groups": [],
    }
    monkeypatch.setattr(build, "_interpret", lambda problem, names: advice)
    answers = [
        "1",
        "which customers churn next month",
        "1",  # a new repository, asked while the description is read
        "1",  # batch, recommended first
        "",  # keep the proposed slug
        "2",  # daily
        "",  # data: the default, synthetic
        "1",  # tables
        "customers",
        "5,000 customers, 15% churn",
        "1",  # a dashboard
        "the retention team reads it every morning",
    ]
    done = _run(tmp_path, monkeypatch, answers)
    assert done.exit_code == 0, done.output
    assert "the list is read once a day" in done.output
    doc = _doc(tmp_path / "churn")
    req = doc["requirements"]
    assert req["status"] == "pending"
    assert req["system_type"] == "batch"
    assert req["sla"] == {"batch": {"cadence": "daily"}}
    assert req["data_sources"][0]["kind"] == "synthetic"
    assert (
        doc["data"]["customers"]["generator"]["story"] == "5,000 customers, 15% churn"
    )
    assert doc["app"]["kind"] == "dashboard"
    assert doc["system"]["repo"] == {"url": "new"}
    assert done.output.index("Where should the code go?") < done.output.index(
        "What type of ML system?"
    )


def test_a_resumed_interview_skips_what_is_answered(tmp_path, monkeypatch, quiet):
    first = _run(tmp_path, monkeypatch, ["2", "2", "1"])
    assert first.exit_code == 0, first.output
    done = _run(tmp_path, monkeypatch, [], "recs-example")
    assert done.exit_code == 0, done.output
    assert "Latency" not in done.output and "Which data" not in done.output


def test_every_write_registers_the_system_for_the_ui(tmp_path, monkeypatch, quiet):
    home = tmp_path / "hopsfs" / "Users" / "meb10000"
    home.mkdir(parents=True)
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(home))
    done = _run(home, monkeypatch, ["2", "1", "1"])
    assert done.exit_code == 0, done.output
    entry = json.loads((home / ".hops" / "builds" / "churn-example.json").read_text())
    assert entry["slug"] == "churn-example"
    assert entry["path"] == "Users/meb10000/churn-example/system.yaml"


def test_a_system_outside_hopsfs_is_not_registered(tmp_path, monkeypatch, quiet):
    home = tmp_path / "hopsfs" / "Users" / "meb10000"
    home.mkdir(parents=True)
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(home))
    elsewhere = tmp_path / "laptop"
    elsewhere.mkdir()
    done = _run(elsewhere, monkeypatch, ["2", "1", "1"])
    assert done.exit_code == 0, done.output
    assert not (home / ".hops").exists()


def test_the_interpretation_drops_what_it_cannot_use(monkeypatch):
    class Done:
        stdout = 'Sure! {"system_type": "sometimes", "slug": "Bad Slug", "feature_groups": ["a", "zz"]}'

    monkeypatch.setattr(build.shutil, "which", lambda name: "/bin/claude")
    monkeypatch.setattr(build.subprocess, "run", lambda *a, **k: Done())
    answer = build._interpret("churn", ["a", "b"])
    assert "system_type" not in answer and "slug" not in answer
    assert answer["feature_groups"] == ["a"]


def test_the_login_banner_is_silenced_for_its_own_thread_only(monkeypatch):
    real = io.StringIO()
    monkeypatch.setattr(sys, "stdout", real)

    in_login = threading.Event()
    prompted = threading.Event()

    class FakeSdk:
        @staticmethod
        def login(**kwargs):
            in_login.set()
            prompted.wait(5)
            print("Logged in to project")
            return "project"

    thread = threading.Thread(target=auth._quiet_login, args=(FakeSdk, {}))
    thread.start()
    in_login.wait(5)
    print("a prompt")
    prompted.set()
    thread.join()
    written = real.getvalue()
    assert "a prompt" in written
    assert "Logged in" not in written


def test_a_system_from_an_older_template_still_resumes(tmp_path, monkeypatch, quiet):
    first = _run(tmp_path, monkeypatch, ["2", "2", "1"])
    assert first.exit_code == 0, first.output
    (tmp_path / "recs-example" / "set.py").unlink()
    done = _run(tmp_path, monkeypatch, [], "recs-example")
    assert done.exit_code == 0, done.output


def test_a_finished_interview_is_offered_as_ready_to_build(
    tmp_path, monkeypatch, quiet
):
    first = _run(tmp_path, monkeypatch, ["2", "1", "1"])
    assert first.exit_code == 0, first.output
    done = _run(tmp_path, monkeypatch, ["1"])
    assert done.exit_code == 0, done.output
    assert "Build churn-example" in done.output
    assert 'claude "/hops-build churn-example"' in done.output
