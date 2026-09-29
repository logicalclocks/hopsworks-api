"""`hops build`: the structured interview that writes system.yaml, then starts the build."""

from __future__ import annotations

import io
import subprocess
import sys
import threading
from typing import TYPE_CHECKING

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli import auth
from hopsworks.cli.commands import build, mlsystem
from hopsworks.cli.main import cli


if TYPE_CHECKING:
    from pathlib import Path


yaml = pytest.importorskip("yaml")


@pytest.fixture
def quiet(monkeypatch):
    """No login, no project listing, no model call, no Claude Code; registrations are recorded."""
    monkeypatch.setattr(build._Prefetch, "run", lambda self: None)
    monkeypatch.setattr(build.shutil, "which", lambda name: None)
    monkeypatch.delenv("TMUX", raising=False)
    registered = []
    monkeypatch.setattr(
        mlsystem,
        "register",
        lambda ctx, path, name=None: registered.append((path, name)),
    )
    return registered


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
    first = done.output.index("What do you want to build?")
    assert done.output.index("1. Start a new ML system") > first
    assert done.output.index("2. Build an example ML system") > first
    assert "3. Help desk agent: answers support questions" in done.output
    doc = _doc(tmp_path / "churn-example")
    assert doc["system"]["example"] == "churn-example"
    assert doc["app"]["wanted"] is True
    assert {s["kind"] for s in doc["requirements"]["data_sources"]} == {"synthetic"}
    assert doc["system"]["repo"] == {"url": "new"}
    assert (
        "Which data" not in done.output and "How are the predictions" not in done.output
    )
    assert 'claude "/hops-build churn-example"' in done.output


def test_a_hopsworks_home_is_never_the_repository(tmp_path, monkeypatch, quiet):
    """Each system in a home is a repository of its own, even in a home an older build made a work tree."""
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "remote",
            "add",
            "origin",
            "https://github.com/o/home",
        ],
        check=True,
    )
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(tmp_path))
    done = _run(tmp_path, monkeypatch, [], "--example", "churn-example")
    assert done.exit_code == 0, done.output
    assert "Where should the code go?" not in done.output
    assert _doc(tmp_path / "churn-example")["system"]["repo"] == {"url": "new"}


def test_the_build_starts_in_the_system_directory(tmp_path, monkeypatch, quiet):
    """Claude Code starts in <slug>/, so it reads the AGENTS.md the template put there."""
    assert (
        _run(tmp_path, monkeypatch, ["1"], "--example", "churn-example").exit_code == 0
    )
    windows = []
    real_run = subprocess.run
    monkeypatch.setenv("TMUX", "/tmp/tmux-1/default,1,0")
    monkeypatch.setattr(build.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        build.subprocess,
        "run",
        lambda cmd, **kw: (
            windows.append(cmd) if cmd[0] == "tmux" else real_run(cmd, **kw)
        ),
    )
    done = CliRunner().invoke(cli, ["build", "churn-example"])
    assert done.exit_code == 0, done.output
    [window] = windows
    assert window[window.index("-c") + 1] == str(tmp_path / "churn-example")
    assert (tmp_path / "churn-example" / "AGENTS.md").is_file()


def test_the_ui_starts_an_example_by_name_and_resumes_it(tmp_path, monkeypatch, quiet):
    done = _run(tmp_path, monkeypatch, ["1"], "--example", "recs-example")
    assert done.exit_code == 0, done.output
    assert "What do you want to build?" not in done.output
    assert _doc(tmp_path / "recs-example")["system"]["repo"] == {"url": "new"}
    again = _run(tmp_path, monkeypatch, [], "--example", "recs-example")
    assert again.exit_code == 0, again.output
    assert "Where should the code go?" not in again.output
    assert 'claude "/hops-build recs-example"' in again.output
    unknown = _run(tmp_path, monkeypatch, [], "--example", "fraud")
    assert unknown.exit_code != 0 and "churn-example" in unknown.output


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


def test_a_finished_interview_registers_the_system_once(tmp_path, monkeypatch, quiet):
    done = _run(tmp_path, monkeypatch, ["2", "1", "1"])
    assert done.exit_code == 0, done.output
    assert quiet == [(tmp_path / "churn-example", "Churn next month")]


def test_a_failed_registration_does_not_lose_the_interview(
    tmp_path, monkeypatch, quiet
):
    def refuse(ctx, path, name=None):
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(mlsystem, "register", refuse)
    done = _run(tmp_path, monkeypatch, ["2", "1", "1"])
    assert done.exit_code == 0, done.output
    assert "Not registered in the project's ML systems" in done.output
    assert (tmp_path / "churn-example" / "system.yaml").exists()


def test_code_in_the_hopsfs_mount_is_registered_by_its_project_path(
    tmp_path, monkeypatch
):
    home = tmp_path / "hopsfs" / "Users" / "meb10000"
    (home / "churn-example").mkdir(parents=True)
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(home))
    assert (
        mlsystem.code_location(home / "churn-example", "churndemo")
        == "/Projects/churndemo/Users/meb10000/churn-example"
    )


def test_code_outside_hopsfs_is_registered_by_its_repository(tmp_path, monkeypatch):
    monkeypatch.delenv("HOPSFS_USER_HOME_DIR", raising=False)
    repo = tmp_path / "laptop"
    (repo / "churn-example").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "remote",
            "add",
            "origin",
            "git@github.com:o/churn.git",
        ],
        check=True,
    )
    assert (
        mlsystem.code_location(repo / "churn-example") == "git@github.com:o/churn.git"
    )
    (tmp_path / "loose").mkdir()
    with pytest.raises(click.ClickException):
        mlsystem.code_location(tmp_path / "loose")


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
    done = _run(tmp_path, monkeypatch, ["3"])
    assert done.exit_code == 0, done.output
    assert "Build churn-example" in done.output
    assert 'claude "/hops-build churn-example"' in done.output
