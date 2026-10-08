"""An ML system built with `hops factory run`: its answers written to system.yaml, what they leave out asked, then the build."""

from __future__ import annotations

import io
import json
import subprocess
import sys
import threading
from typing import TYPE_CHECKING

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli import auth, session
from hopsworks.cli.commands import build, mlsystem
from hopsworks.cli.main import cli
from hopsworks_common.core import factory_api


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
        lambda ctx, path, name=None, factory=None: registered.append((path, name)),
    )
    return registered


# A factory whose only question is the slug, so every answer reaches the ML system build.
FACTORY = """\
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: ml-test
title: ML test
form:
  sections:
    - id: system
      title: System
      fields:
        - {id: slug, type: slug, label: Name, required: true}
phases:
  - {key: reqs, label: Requirements}
build:
  builtin: mlsystem
"""


def _run(tmp_path: Path, monkeypatch, answers: list[str], *args: str):
    """`hops factory run ml-test --no-launch ARGS`, typing `answers` at the prompts."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        factory_api,
        "_get",
        lambda name, version=None: {
            "name": name,
            "version": 1,
            "enabled": True,
            "definition": FACTORY,
            "spec": yaml.safe_load(FACTORY),
        },
    )
    monkeypatch.setattr(session, "get_project", lambda ctx: None)
    return CliRunner().invoke(
        cli,
        ["factory", "run", "ml-test", "--no-launch", *args],
        input="\n".join(answers) + "\n",
    )


def _example(tmp_path: Path, monkeypatch, answers: list[str], name: str):
    """An example system, as the Factory's presets start one."""
    return _run(
        tmp_path,
        monkeypatch,
        answers,
        "--answers",
        _answers(tmp_path, slug=name, example=name),
    )


def _doc(target: Path) -> dict:
    return yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8"))


def test_an_example_asks_only_where_the_code_goes(tmp_path, monkeypatch, quiet):
    done = _example(tmp_path, monkeypatch, ["1"], "churn-example")
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


def test_the_helpdesk_example_keeps_its_llm_in_account_env_vars(
    tmp_path, monkeypatch, quiet
):
    """The key goes to the account settings, never to the screen or system.yaml."""
    from hopsworks_common.core import env_var_api

    saved = {}

    class Api:
        def get_env_vars(self, include_value=True):
            return []

        def set_env_var(self, name, value=None, visibility=None, **_):
            saved[name] = (value, visibility)

    monkeypatch.setattr(env_var_api, "EnvVarsApi", Api)
    key = "not-a-real-key-0123"
    answers = ["https://llm.example/v1", "", key, "1"]
    done = _example(tmp_path, monkeypatch, answers, "helpdesk-example")
    assert done.exit_code == 0, done.output
    assert saved == {
        "LLM_URL": ("https://llm.example/v1", "PRIVATE"),
        "LLM_MODEL": ("gpt-4o-mini", "PRIVATE"),
        "LLM_API_KEY": (key, "PRIVATE"),
    }
    assert key not in done.output
    spec = (tmp_path / "helpdesk-example" / "system.yaml").read_text(encoding="utf-8")
    assert key not in spec
    agent = _doc(tmp_path / "helpdesk-example")["inference"]["agent"]
    assert agent["deployment"] == "helpdeskagent"
    assert agent["llm"]["api_key_env"] == "LLM_API_KEY"


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
    done = _example(tmp_path, monkeypatch, [], "churn-example")
    assert done.exit_code == 0, done.output
    assert "Where should the code go?" not in done.output
    assert _doc(tmp_path / "churn-example")["system"]["repo"] == {"url": "new"}


def test_the_build_starts_in_the_system_directory(tmp_path, monkeypatch, quiet):
    """Claude Code starts in <slug>/, so it reads the AGENTS.md the template put there."""
    assert _example(tmp_path, monkeypatch, ["1"], "churn-example").exit_code == 0
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
    done = CliRunner().invoke(cli, ["factory", "run", "ml-test", "churn-example"])
    assert done.exit_code == 0, done.output
    [window] = windows
    assert window[window.index("-c") + 1] == str(tmp_path / "churn-example")
    agents = (tmp_path / "churn-example" / "AGENTS.md").read_text(encoding="utf-8")
    # Every change, a fix or maintenance included, keeps system.yaml current.
    assert "must always reflect the current state of the ML system" in agents
    # Logs stay out of the system's repository.
    assert "Logs/factory/<slug>/" in agents
    ignored = (tmp_path / "churn-example" / ".gitignore").read_text(encoding="utf-8")
    assert "logs-*/" in ignored.splitlines() and "*.log" in ignored.splitlines()


def test_the_ui_starts_an_example_by_name_and_resumes_it(tmp_path, monkeypatch, quiet):
    done = _example(tmp_path, monkeypatch, ["1"], "recs-example")
    assert done.exit_code == 0, done.output
    assert _doc(tmp_path / "recs-example")["system"]["repo"] == {"url": "new"}
    again = _example(tmp_path, monkeypatch, [], "recs-example")
    assert again.exit_code == 0, again.output
    assert "Where should the code go?" not in again.output
    assert 'claude "/hops-build recs-example"' in again.output
    unknown = _example(tmp_path, monkeypatch, [], "fraud")
    assert unknown.exit_code != 0 and "churn-example" in unknown.output


def _answers(tmp_path: Path, **answers) -> str:
    path = tmp_path.parent / f"{tmp_path.name}-answers.json"
    path.write_text(json.dumps(answers), encoding="utf-8")
    return str(path)


def test_the_uis_answers_leave_nothing_to_ask(tmp_path, monkeypatch, quiet):
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(tmp_path))
    path = _answers(
        tmp_path,
        slug="late-orders",
        description="which orders will ship late",
        system_type="batch",
        sla={"batch": {"cadence": "daily"}},
        consumers="ui",
        data_sources=[
            {
                "name": "orders",
                "kind": "synthetic",
                "shape": "events",
                "story": "10k orders",
            }
        ],
        app={"wanted": True, "kind": "dashboard", "description": "ops reads it"},
        repo={"create": False, "url": "https://gitlab.com/acme/late-orders.git"},
        reference_code="https://github.com/acme/late-orders-demo",
        monitoring={"feature_logging": True, "watch": " drift in order value "},
    )
    done = _run(tmp_path, monkeypatch, [], "--answers", path)
    assert done.exit_code == 0, done.output
    assert "?" not in done.output.replace("Where should the code go?", "")
    doc = _doc(tmp_path / "late-orders")
    assert doc["system"]["repo"] == {
        "url": "https://gitlab.com/acme/late-orders.git",
        "provider": "gitlab",
    }
    req = doc["requirements"]
    assert req["reference_code"] == "https://github.com/acme/late-orders-demo"
    assert req["monitoring"] == {
        "feature_logging": True,
        "watch": "drift in order value",
    }
    # A daily run fires at the UI's default time when the answers name none.
    assert (req["system_type"], req["sla"]) == (
        "batch",
        {"batch": {"cadence": "daily", "at": "02:00"}},
    )
    assert req["data_sources"] == [
        {
            "name": "orders",
            "kind": "synthetic",
            "shape": "events",
            "status": "needs_generation",
        }
    ]
    assert doc["data"]["orders"]["generator"]["story"] == "10k orders"
    assert doc["app"] == {
        "kind": "dashboard",
        "description": "ops reads it",
        "wanted": True,
        "status": "pending",
    }
    assert doc["system"]["version"] == "0.1.0"
    assert quiet == [(tmp_path / "late-orders", "Late orders")]


def test_the_uis_answers_override_an_example(tmp_path, monkeypatch, quiet):
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(tmp_path))
    path = _answers(
        tmp_path,
        slug="my-helpdesk",
        example="helpdesk-example",
        description="answers questions about our returns policy",
        data_sources=[
            {"name": "docs", "kind": "file"},
            {
                "name": "user_events",
                "kind": "synthetic",
                "shape": "events",
                "story": "50 users",
            },
        ],
        llm="account",
    )
    done = _run(tmp_path, monkeypatch, [], "--answers", path)
    assert done.exit_code == 0, done.output
    assert "API key" not in done.output
    doc = _doc(tmp_path / "my-helpdesk")
    assert doc["system"]["example"] == "helpdesk-example"
    assert doc["requirements"]["description"].startswith("answers questions about")
    assert doc["inference"]["agent"]["deployment"] == "helpdeskagent"
    assert doc["inference"]["agent"]["llm"]["api_key_env"] == "LLM_API_KEY"
    docs, events = doc["requirements"]["data_sources"]
    assert docs == {"name": "docs", "kind": "file", "status": "needs_download"}
    assert doc["data"]["user_events"]["generator"]["story"] == "50 users"
    assert doc["data"]["user_events"]["writes"]["feature_group"] == "user_events"
    # A second Create resumes the system rather than failing on it.
    again = _run(tmp_path, monkeypatch, [], "--answers", path)
    assert again.exit_code == 0 and "resuming" in again.output


def test_answers_are_checked(tmp_path, monkeypatch, quiet):
    bad = _run(
        tmp_path, monkeypatch, [], "--answers", _answers(tmp_path, slug="Has Space")
    )
    assert bad.exit_code != 0 and "lowercase" in bad.output
    agent = _run(
        tmp_path,
        monkeypatch,
        [],
        "--answers",
        _answers(
            tmp_path,
            slug="helper",
            system_type="agent",
            monitoring={"feature_logging": True},
        ),
    )
    assert agent.exit_code != 0 and "batch or realtime" in agent.output


def test_a_resumed_interview_skips_what_is_answered(tmp_path, monkeypatch, quiet):
    first = _example(tmp_path, monkeypatch, ["1"], "recs-example")
    assert first.exit_code == 0, first.output
    done = _run(tmp_path, monkeypatch, [], "recs-example")
    assert done.exit_code == 0, done.output
    assert "Latency" not in done.output and "Which data" not in done.output


def test_a_finished_interview_registers_the_system_once(tmp_path, monkeypatch, quiet):
    done = _example(tmp_path, monkeypatch, ["1"], "churn-example")
    assert done.exit_code == 0, done.output
    assert quiet == [(tmp_path / "churn-example", "Churn next month")]


def test_a_failed_registration_does_not_lose_the_interview(
    tmp_path, monkeypatch, quiet
):
    def refuse(ctx, path, name=None, factory=None):
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(mlsystem, "register", refuse)
    done = _example(tmp_path, monkeypatch, ["1"], "churn-example")
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
    first = _example(tmp_path, monkeypatch, ["1"], "recs-example")
    assert first.exit_code == 0, first.output
    (tmp_path / "recs-example" / "set.py").unlink()
    done = _run(tmp_path, monkeypatch, [], "recs-example")
    assert done.exit_code == 0, done.output


def test_an_agent_with_data_sources_keeps_its_account_llm(tmp_path, monkeypatch, quiet):
    path = _answers(
        tmp_path,
        slug="faq-agent",
        system_type="agent",
        description="answers questions about our products",
        data_sources=[{"name": "products", "kind": "feature_group", "version": 2}],
        llm="account",
    )
    done = _run(tmp_path, monkeypatch, [], "--answers", path)
    assert done.exit_code == 0, done.output
    doc = _doc(tmp_path / "faq-agent")
    # The answers chose the account LLM, so the CLI never asks for one.
    assert "account environment variables" not in done.output
    assert doc["inference"]["agent"]["llm"]["model_env"] == "LLM_MODEL"
    assert doc["requirements"]["data_sources"][0]["kind"] == "feature_group"


def test_a_factory_forms_answers_are_normalized():
    answers = build._normalized(
        {
            "sources": {
                "feature_groups": [{"name": "orders", "version": 2}],
                "synthetic": [
                    {
                        "shape": "events",
                        "story": "Clickstream for a web shop: 5,000 visitors",
                    },
                    {"name": "customers", "story": "5k customers"},
                ],
                "files": [{"name": "docs"}],
            },
            "app": {"kind": "none", "description": "unused"},
            "sla": {"batch": {"cadence": "hourly", "at": "02:00"}},
            "monitoring": {"feature_logging": False, "watch": " "},
        },
        "batch",
    )
    assert answers["data_sources"] == [
        {"name": "orders", "kind": "feature_group", "version": 2},
        {
            "name": "clickstream_web_shop",
            "kind": "synthetic",
            "shape": "events",
            "story": "Clickstream for a web shop: 5,000 visitors",
        },
        {
            "name": "customers",
            "kind": "synthetic",
            "shape": "batch",
            "story": "5k customers",
        },
        {"name": "docs", "kind": "file"},
    ]
    assert answers["app"] == {"wanted": False}
    assert answers["consumers"] == "api"
    # 02:00 is a daily time; an hourly run fires on the hour unless told a minute.
    assert answers["sla"]["batch"]["at"] == ":00"
    assert "monitoring" not in answers and "sources" not in answers


def test_a_typed_source_name_becomes_an_identifier():
    answers = build._normalized(
        {
            "sources": {
                "synthetic": [
                    {"name": "Clickstream data", "story": "clicks on a retail website"},
                    {"name": "2024 orders", "story": "orders"},
                ],
                "files": [{"name": "Help desk docs"}, {}],
            }
        },
        "agent",
    )
    assert [s["name"] for s in answers["data_sources"]] == [
        "clickstream_data",
        "s_2024_orders",
        "help_desk_docs",
        "files_2",
    ]
