"""``hops deployment`` env, set-env, versions, rollback, predict --via-api and delete, end to end.

Each command runs through the real login, SDK and REST client against a local
HTTPS server that keeps the serving API of its projects in memory
(``serving_server.ServingBackend``). Transport failures come from a real TLS
proxy in front of it (``serving_server.CutProxy``): a stalled answer, a
connection dropped after the body was forwarded, a write forwarded only after
the client gave up.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from click.testing import CliRunner
from hopsworks.cli import config
from hopsworks.cli.commands import deployment as deployment_cmd
from hopsworks.cli.main import cli
from hopsworks_common import client

from tests.cli.serving_server import CutProxy, Deployment, ServingBackend, tls_context


@pytest.fixture
def tls(tmp_path):
    return tls_context(tmp_path)


@pytest.fixture
def backend(tls, tmp_home, monkeypatch):
    """Project ``demo`` (id 119) with deployment ``echo`` (id 7, A=1), and a saved login to it."""
    for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setattr(deployment_cmd, "_POLL_SECONDS", 0.2)
    server = ServingBackend(tls)
    server.add(119, "demo", Deployment(7, "echo", {"A": "1"}))
    config.save(config.HopsConfig(host=server.url, api_key="k", project="demo"))
    yield server
    client._stop()
    server.stop()


@pytest.fixture
def proxies(tls):
    started = []

    def start(upstream: str, cut) -> CutProxy:
        proxy = CutProxy(tls, upstream, cut)
        started.append(proxy)
        return proxy

    yield start
    for proxy in started:
        proxy.stop()


def run(*args, root=()):
    """One ``hops`` process: the SDK client it made is gone when it ends."""
    try:
        return CliRunner().invoke(cli, [*root, "deployment", *args])
    finally:
        client._stop()


def run_json(*args, root=()):
    result = run(*args, "--json", root=root)
    return result, json.loads(result.stdout)


def via(proxy: CutProxy):
    return ("--host", proxy.url)


def test_env_prints_sorted_pairs_and_json(backend):
    backend.deployment(119, "echo").versions[0]["env"] = {"B": "2", "A": "1"}
    result = run("env", "echo")
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == ["A=1", "B=2"]
    _, out = run_json("env", "echo")
    assert out == {"A": "1", "B": "2"}


def test_set_env_merges_and_saves_a_new_version(backend):
    backend.deployment(119, "echo").versions[0]["env"] = {"KEEP": "1", "DROP": "x"}
    result, out = run_json("set-env", "echo", "NEW=secret-value", "--unset", "DROP")
    assert result.exit_code == 0, result.output
    d = backend.deployment(119, "echo")
    assert (d.active, d.env()) == (2, {"KEEP": "1", "NEW": "secret-value"})
    assert (out["result"], out["accepted"], out["write"], out["phase"]) == (
        "applied",
        True,
        "ok",
        "done",
    )
    assert (out["set"], out["unset"]) == (["NEW"], ["DROP"])
    assert (out["before"]["version"], out["after"]["version"]) == (1, 2)
    assert out["after"]["digest"] == out["target_digest"] != out["before"]["digest"]
    assert out["after"]["revision"] != out["before"]["revision"]
    assert "room" not in out
    assert "secret-value" not in result.output
    assert backend.writes() == [("PUT", "/hopsworks-api/api/project/119/serving")]


def test_set_env_never_uploads_a_local_file_named_like_the_script(
    backend, tmp_path, monkeypatch
):
    """Hopsworks reports the script as a bare name; a file of that name here must not replace it."""
    workdir = tmp_path / "ml-system"
    workdir.mkdir()
    (workdir / "predictor.py").write_text("raise SystemExit('not this one')\n")
    monkeypatch.chdir(workdir)
    result, out = run_json("set-env", "echo", "A=2")
    assert result.exit_code == 0, result.output
    assert out["result"] == "applied"
    assert backend.writes() == [("PUT", "/hopsworks-api/api/project/119/serving")]
    assert Path.cwd() == workdir


def test_set_env_unchanged_writes_nothing(backend):
    result = run("set-env", "echo", "A=1", "--unset", "MISSING")
    assert result.exit_code == 0, result.output
    assert result.output.startswith("Unchanged")
    assert backend.writes() == []


def test_set_env_refuses_reserved_names(backend):
    for args in (["SERVING_SCHEMA_ID=x"], ["--unset", "SERVING_SCRIPT_KIND"]):
        result = run("set-env", "echo", *args)
        assert result.exit_code == 2
        assert "set by Hopsworks" in result.output
    assert backend.requests == []


def test_set_env_on_a_stopped_deployment_saves_and_stays_stopped(backend):
    backend.deployment(119, "echo").status = "Stopped"
    result, out = run_json("set-env", "echo", "A=2")
    assert result.exit_code == 0, result.output
    assert (out["result"], out["after"]["version"], out["after"]["status"]) == (
        "applied",
        2,
        "Stopped",
    )


def test_set_env_refuses_while_updating(backend):
    backend.deployment(119, "echo").status = "Updating"
    result = run("set-env", "echo", "A=2")
    assert result.exit_code == 1
    assert "is Updating" in result.output
    assert backend.writes() == []


def test_backend_refusal_is_passed_on_as_refused(backend):
    """A quota refusal from Hopsworks: proven nothing saved, its message as Hopsworks gave it."""
    backend.reject_writes = (
        429,
        240080,
        "Project quota exceeded: this deployment needs 1.25 CPU cores but only 1.04 CPU cores are free",
    )
    result, out = run_json("set-env", "echo", "A=2")
    assert result.exit_code == 1
    assert (out["result"], out["accepted"], out["write"], out["http_status"]) == (
        "refused",
        False,
        "rejected",
        429,
    )
    assert "Project quota exceeded" in out["error"]
    assert out["after"]["version"] == 1
    text = run("set-env", "echo", "A=2")
    assert "Refused, nothing saved" in text.output
    assert "Project quota exceeded" in text.output


def test_a_write_dropped_after_its_body_was_forwarded_is_read_back_applied(
    backend, proxies
):
    """The proxy forwards the save, then resets the client: the read shows it applied."""
    proxy = proxies(backend.url, lambda m, p: "drop" if m == "PUT" else None)
    result, out = run_json("set-env", "echo", "A=2", root=via(proxy))
    assert result.exit_code == 0, result.output
    assert [c[:2] for c in proxy.cuts] == [
        ("PUT", "/hopsworks-api/api/project/119/serving")
    ]
    assert (out["result"], out["accepted"], out["write"]) == ("applied", True, "sent")
    assert out["error"]
    assert backend.deployment(119, "echo").active == 2


@pytest.mark.parametrize("answer", [None, 502], ids=["reset", "gateway_page"])
def test_a_lost_write_still_in_flight_is_unknown_never_refused(
    backend, proxies, answer
):
    """The save reaches Hopsworks only after the client lost it and read the old version.

    On 219cbeaa this read made the save "refused, nothing saved"; Hopsworks
    then applied it.
    """
    proxy = proxies(
        backend.url,
        lambda m, p: ("delay", 1.5, answer) if m == "PUT" else None,
    )
    result, out = run_json("set-env", "echo", "A=2", root=via(proxy))
    assert result.exit_code == 1
    assert (out["result"], out["accepted"], out["write"]) == ("unknown", None, "sent")
    assert out["after"]["version"] == 1
    if answer is not None:
        assert out["http_status"] == answer
        assert "without an answer from Hopsworks" in out["error"]
    assert proxy.forwarded.wait(10)
    assert backend.deployment(119, "echo").active == 2
    text = run("versions", "echo", "--json")
    assert [r["version"] for r in json.loads(text.stdout) if r["active"]] == [2]


def test_a_lost_write_text_says_it_may_still_apply(backend, proxies):
    proxy = proxies(backend.url, lambda m, p: ("delay", 1.5) if m == "PUT" else None)
    result = run("set-env", "echo", "A=2", root=via(proxy))
    assert result.exit_code == 1
    assert "Outcome unknown" in result.output and "may still apply" in result.output
    assert "Refused" not in result.output
    assert proxy.forwarded.wait(10)


def test_not_ready_names_the_state_and_the_way_back(backend):
    """A rollout that stays Updating: the wait ends inside --timeout, with a final read."""
    backend.rollout_status = "Updating"
    started = time.monotonic()
    result = run("set-env", "echo", "A=2", "--timeout", "3")
    assert time.monotonic() - started < 6
    assert result.exit_code == 1
    assert "not ready" in result.output and "Updating" in result.output
    assert "hops deployment rollback echo 1" in result.output


def test_a_stalled_rollout_read_is_cut_off_at_the_timeout(backend, proxies):
    """Hopsworks accepts the save, then a state read is accepted and never answered."""
    backend.rollout_status = "Updating"
    reads_after_save = []

    def cut(method, path):
        if method == "PUT":
            reads_after_save.append(path)
            return None
        if reads_after_save and path.endswith("/serving/7"):
            reads_after_save.append(path)
            return "stall" if len(reads_after_save) > 2 else None
        return None

    proxy = proxies(backend.url, cut)
    started = time.monotonic()
    result, out = run_json("set-env", "echo", "A=2", "--timeout", "4", root=via(proxy))
    elapsed = time.monotonic() - started
    assert elapsed < 6, elapsed
    assert ("GET", "/hopsworks-api/api/project/119/serving/7", "stall") in proxy.cuts
    assert result.exit_code == 1
    assert (out["result"], out["accepted"], out["phase"]) == ("unknown", True, "wait")
    assert "read failed" in out["error"]


def test_a_stalled_login_is_cut_off_before_anything_is_sent(backend, proxies):
    proxy = proxies(backend.url, lambda m, p: "stall")
    started = time.monotonic()
    result = run("set-env", "echo", "A=2", "--timeout", "3", root=via(proxy))
    assert time.monotonic() - started < 5
    assert result.exit_code == 1
    assert backend.requests == []


def test_versions_lists_keys_and_digests_not_values(backend):
    d = backend.deployment(119, "echo")
    d.versions[0]["env"] = {"A": "secret-value"}
    d.versions.append({"version": 2, "env": {}})
    d.active = 2
    result, rows = run_json("versions", "echo")
    assert result.exit_code == 0, result.output
    assert [(r["version"], r["active"], r["env_keys"]) for r in rows] == [
        (2, True, []),
        (1, False, ["A"]),
    ]
    assert rows[0]["env_digest"] != rows[1]["env_digest"]
    assert "secret-value" not in result.output


def test_rollback_reaches_the_version_and_reports_the_read(backend):
    d = backend.deployment(119, "echo")
    d.versions.append({"version": 2, "env": {"A": "2"}})
    d.active = 2
    result, out = run_json("rollback", "echo", "1")
    assert result.exit_code == 0, result.output
    assert (out["result"], out["accepted"], out["write"]) == ("applied", True, "ok")
    assert (out["before"]["version"], out["after"]["version"]) == (2, 1)

    result = run("rollback", "echo", "1")
    assert result.exit_code == 0 and result.output.startswith("Unchanged")
    result = run("rollback", "echo", "7")
    assert result.exit_code == 1 and "no version 7" in result.output
    assert [m for m, _ in backend.writes()] == ["POST"]


def test_rollback_refused_by_hopsworks_and_lost_in_flight(backend, proxies):
    d = backend.deployment(119, "echo")
    d.versions.append({"version": 2, "env": {"A": "2"}})
    d.active = 2
    backend.reject_writes = (429, 240080, "Project quota exceeded")
    result, out = run_json("rollback", "echo", "1")
    assert (out["result"], out["accepted"], out["write"]) == (
        "refused",
        False,
        "rejected",
    )
    assert d.active == 2

    backend.reject_writes = None
    proxy = proxies(backend.url, lambda m, p: ("delay", 1.5) if m == "POST" else None)
    result, out = run_json("rollback", "echo", "1", root=via(proxy))
    assert (out["result"], out["accepted"]) == ("unknown", None)
    assert proxy.forwarded.wait(10)
    assert d.active == 1


def test_predict_via_api_timing_reports_status_and_ms(backend):
    result, out = run_json(
        "predict", "echo", "--data", '{"instances": [[1, 2]]}', "--via-api", "--timing"
    )
    assert result.exit_code == 0, result.output
    assert out["status"] == 200 and isinstance(out["ms"], int)
    assert out["response"] == {"predictions": [[1, 2]]}
    assert backend.writes() == [
        ("POST", "/hopsworks-api/api/project/119/inference/models/echo:predict")
    ]


def test_predict_via_api_on_a_stalled_endpoint_ends_at_its_bound(
    backend, proxies, monkeypatch
):
    monkeypatch.setattr(deployment_cmd, "_VIA_API_SECONDS", 2)
    proxy = proxies(backend.url, lambda m, p: "stall" if "inference" in p else None)
    started = time.monotonic()
    result, out = run_json(
        "predict", "echo", "--data", "{}", "--via-api", "--timing", root=via(proxy)
    )
    assert time.monotonic() - started < 5
    assert result.exit_code == 1
    assert out["status"] is None and out["ms"] < 4000


def test_a_serving_only_key_uses_the_project_id_it_is_given(backend, monkeypatch):
    backend.project_readable = False
    result = run("versions", "echo")
    assert result.exit_code == 1
    assert "HOPSWORKS_PROJECT_ID" in result.output
    monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "119")
    result = run("versions", "echo")
    assert result.exit_code == 0, result.output


def test_a_project_read_by_name_wins_over_a_given_id(backend, monkeypatch):
    monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "4242")
    result = run("versions", "echo")
    assert result.exit_code == 0, result.output
    assert all("/project/4242/" not in p for _, p in backend.requests)


# A project or host override with a saved project id: the id belongs to the saved
# project on the saved host, and a key that cannot read the new project by name
# must not fall back to it.


@pytest.fixture
def two_projects(backend):
    """The owner's projects alpha (101) and beta (202), each with a deployment echo; alpha saved with its id."""
    backend.projects.clear()
    backend.add(101, "alpha", Deployment(11, "echo", {"A": "1"}))
    backend.add(202, "beta", Deployment(22, "echo", {"B": "1"}))
    for project in backend.projects.values():
        project["deployments"]["echo"].status = "Stopped"
    backend.project_readable = False
    config.save(
        config.HopsConfig(
            host=backend.url, api_key="k", project="alpha", project_id=101
        )
    )
    return backend


def test_delete_under_another_project_never_reaches_the_saved_project(
    two_projects, monkeypatch
):
    """Repro of gate H1 on 219cbeaa: HOPSWORKS_PROJECT=beta deleted alpha's echo."""
    monkeypatch.setenv("HOPSWORKS_PROJECT", "beta")
    result = run("delete", "echo", "--yes")
    assert two_projects.writes() == []
    assert set(two_projects.projects[101]["deployments"]) == {"echo"}
    assert result.exit_code == 1
    assert "HOPSWORKS_PROJECT_ID" in result.output


@pytest.mark.parametrize(
    "env, root",
    [
        ({"PROJECT_NAME": "beta"}, ()),
        ({}, ("--project", "beta")),
        ({"HOPSWORKS_PROJECT": "beta"}, ("--project", "beta")),
    ],
    ids=["PROJECT_NAME", "project_flag", "project_flag_repeating_env"],
)
def test_no_project_override_reaches_the_saved_project_id(
    two_projects, monkeypatch, env, root
):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    result = run("delete", "echo", "--yes", root=root)
    assert result.exit_code == 1
    assert two_projects.writes() == []


@pytest.mark.parametrize("override", ["HOPSWORKS_HOST", "--host"])
def test_no_host_override_reaches_the_saved_project_id(
    two_projects, tls, monkeypatch, override
):
    """Another cluster with a project of the same name: the saved id is the old cluster's."""
    other = ServingBackend(tls)
    try:
        other.add(101, "other-team", Deployment(11, "echo", {}))
        other.add(303, "alpha", Deployment(33, "echo", {}))
        for project in other.projects.values():
            project["deployments"]["echo"].status = "Stopped"
        other.project_readable = False
        root = ()
        if override == "--host":
            root = ("--host", other.url)
        else:
            monkeypatch.setenv(override, other.url)
        result = run("delete", "echo", "--yes", root=root)
        assert result.exit_code == 1
        assert "HOPSWORKS_PROJECT_ID" in result.output
        assert other.writes() == [] and two_projects.writes() == []
    finally:
        other.stop()


def test_an_exported_id_holds_for_the_project_it_was_exported_with(
    two_projects, monkeypatch
):
    monkeypatch.setenv("HOPSWORKS_PROJECT", "beta")
    monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "202")
    result = run("delete", "echo", "--yes", root=("--project", "beta"))
    assert result.exit_code == 0, result.output
    assert two_projects.writes() == [
        ("DELETE", "/hopsworks-api/api/project/202/serving/22")
    ]
    assert set(two_projects.projects[101]["deployments"]) == {"echo"}


# A response that starts and then arrives one byte a second never trips a socket
# timeout; the command's deadline must still end it.


def test_a_trickling_rollout_read_is_cut_off_at_the_timeout(backend, proxies):
    backend.rollout_status = "Updating"
    saved = []

    def cut(method, path):
        if method == "PUT":
            saved.append(path)
        elif saved and path.endswith("/serving/7"):
            saved.append(path)
            return "trickle" if len(saved) > 2 else None
        return None

    proxy = proxies(backend.url, cut)
    started = time.monotonic()
    result, out = run_json("set-env", "echo", "A=2", "--timeout", "4", root=via(proxy))
    assert time.monotonic() - started < 5.5
    assert ("GET", "/hopsworks-api/api/project/119/serving/7", "trickle") in proxy.cuts
    assert (out["result"], out["accepted"], out["phase"]) == ("unknown", True, "wait")
    assert "still arriving" in out["error"]


def test_a_trickling_write_answer_cut_off_at_the_deadline_is_unknown(backend, proxies):
    """The save's answer trickles past the deadline and Hopsworks gets the save only later."""
    proxy = proxies(backend.url, lambda m, p: ("trickle", 5) if m == "PUT" else None)
    started = time.monotonic()
    result, out = run_json("set-env", "echo", "A=2", "--timeout", "4", root=via(proxy))
    assert time.monotonic() - started < 5.5
    assert (out["result"], out["accepted"], out["write"]) == ("unknown", None, "sent")
    assert proxy.forwarded.wait(10)
    assert backend.deployment(119, "echo").active == 2


def test_a_trickling_login_is_cut_off_before_anything_is_sent(backend, proxies):
    proxy = proxies(backend.url, lambda m, p: "trickle")
    started = time.monotonic()
    result = run("set-env", "echo", "A=2", "--timeout", "3", root=via(proxy))
    assert time.monotonic() - started < 4.5
    assert result.exit_code == 1
    assert ("GET", "/hopsworks-api/api/variables/versions", "trickle") in proxy.cuts
    assert backend.writes() == []


def test_a_trickling_via_api_answer_ends_at_its_bound(backend, proxies, monkeypatch):
    monkeypatch.setattr(deployment_cmd, "_VIA_API_SECONDS", 2)
    proxy = proxies(backend.url, lambda m, p: "trickle" if "inference" in p else None)
    started = time.monotonic()
    result, out = run_json(
        "predict", "echo", "--data", "{}", "--via-api", "--timing", root=via(proxy)
    )
    assert time.monotonic() - started < 3.5
    assert result.exit_code == 1
    assert out["status"] is None and out["ms"] < 3000
