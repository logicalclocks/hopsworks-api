"""CliRunner tests for ``hops deployment env``, ``set-env``, ``versions``, ``rollback`` and ``predict --via-api --timing``.

The deployment is a small stateful fake: a save as a new version or a rollback
changes what the next read returns, so each test checks that the result is the
one read back, not the one attempted.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import auth, config
from hopsworks.cli.commands import deployment as deployment_cmd
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import RestAPIError


GIB = 1024**3


class FakeDeployment:
    """A deployment whose versions, state and revision move as Hopsworks moves them."""

    def __init__(self, env=None, status="Running", outcome="Running"):
        self.name = "echo"
        self.versions = [{"version": 1, "env": dict(env or {})}]
        self.active = 1
        self.status = status
        self.revision = "aaaa0001"
        self.outcome = outcome  # status a rollout ends in
        self.save_error = None
        self.apply_save = True
        self.saves = []
        self.rollbacks = []
        self.env_vars = dict(env or {})
        limits = SimpleNamespace(cores=0.5, memory=512)
        self.predictor = SimpleNamespace(
            resources=SimpleNamespace(limits=limits),
            scaling_configuration=SimpleNamespace(min_instances=1),
        )
        self.transformer = None

    def _roll(self):
        if self.status not in ("Stopped",):
            self.status = self.outcome
            self.revision = f"rev{len(self.saves) + len(self.rollbacks)}"

    def save(self, await_update=600, new_version=False):
        self.saves.append((dict(self.env_vars), await_update, new_version))
        if self.apply_save:
            number = max(v["version"] for v in self.versions) + 1
            self.versions.append({"version": number, "env": dict(self.env_vars)})
            self.active = number
            self._roll()
        if self.save_error is not None:
            raise self.save_error

    def rollback(self, version, await_update=600):
        self.rollbacks.append((version, await_update))
        self.active = version
        self.env_vars = dict(
            next(v for v in self.versions if v["version"] == version)["env"]
        )
        self._roll()

    def get_state(self):
        return SimpleNamespace(
            status=self.status,
            revision=self.revision,
            available_predictor_instances=1 if self.status == "Running" else 0,
            available_transformer_instances=None,
            condition=SimpleNamespace(reason="probe failed"),
        )

    def get_versions(self):
        return [
            SimpleNamespace(
                version=v["version"],
                active=v["version"] == self.active,
                created="2026-11-20T10:00:00Z",
                created_by="hopsabl2",
                predictor_env_vars=[f"{k}={val}" for k, val in v["env"].items()],
            )
            for v in sorted(self.versions, key=lambda v: -v["version"])
        ]


@pytest.fixture
def serving(mock_project, monkeypatch):
    monkeypatch.setattr(deployment_cmd, "_POLL_SECONDS", 0)
    ms = mock.MagicMock()
    mock_project.get_model_serving.return_value = ms
    return ms


@pytest.fixture
def quota():
    """The project's compute quota read: 4 CPU and 8 GiB, of which ``used`` is taken."""
    rest = mock.MagicMock()
    state = {"cpu_used": 1.0, "mem_used": 2 * GIB, "error": None}

    def send(method, path, **kwargs):
        if state["error"] is not None:
            raise state["error"]
        assert path[-2:] == ["kube", "quota"]
        return {
            "method": "RESOURCE_QUOTA",
            "cpu": {"limit": 4.0, "used": state["cpu_used"]},
            "memory": {"limit": 8.0 * GIB, "used": state["mem_used"]},
        }

    rest._send_request.side_effect = send
    rest._project_id = "119"
    with mock.patch("hopsworks_common.client._get_instance", return_value=rest):
        yield state


def _error(status, code=None, msg="refused"):
    response = mock.MagicMock()
    response.status_code = status
    response.json.return_value = {"errorCode": code, "usrMsg": msg}
    return RestAPIError("http://h/x", response)


def run(*args):
    return CliRunner().invoke(cli, ["deployment", *args])


def test_env_prints_sorted_pairs_and_json(serving):
    serving.get_deployment.return_value = FakeDeployment({"B": "2", "A": "1"})
    result = run("env", "echo")
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == ["A=1", "B=2"]
    result = run("env", "echo", "--json")
    assert json.loads(result.output) == {"A": "1", "B": "2"}


def test_set_env_merges_and_saves_a_new_version(serving, quota):
    d = FakeDeployment({"KEEP": "1", "DROP": "x"})
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "NEW=secret-value", "--unset", "DROP", "--json")
    assert result.exit_code == 0, result.output
    assert d.saves == [({"KEEP": "1", "NEW": "secret-value"}, 0, True)]
    out = json.loads(result.output)
    assert out["result"] == "applied"
    assert out["accepted"] is True
    assert out["phase"] == "done"
    assert (out["set"], out["unset"]) == (["NEW"], ["DROP"])
    assert (out["before"]["version"], out["after"]["version"]) == (1, 2)
    assert out["after"]["digest"] == out["target_digest"] != out["before"]["digest"]
    assert out["after"]["revision"] != out["before"]["revision"]
    assert out["room"]["ok"] is True
    assert "secret-value" not in result.output


def test_set_env_unchanged_writes_nothing(serving, quota):
    d = FakeDeployment({"A": "1"})
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=1", "--unset", "MISSING")
    assert result.exit_code == 0, result.output
    assert result.output.startswith("Unchanged")
    assert d.saves == []


def test_set_env_refuses_reserved_names(serving):
    d = FakeDeployment()
    serving.get_deployment.return_value = d
    for args in (["SERVING_SCHEMA_ID=x"], ["--unset", "SERVING_SCRIPT_KIND"]):
        result = run("set-env", "echo", *args)
        assert result.exit_code == 2
        assert "set by Hopsworks" in result.output
    assert d.saves == []


def test_set_env_refuses_without_room_and_names_the_numbers(serving, quota):
    quota["cpu_used"] = 3.5  # 0.5 free, the deployment needs 0.5 twice over
    d = FakeDeployment({"A": "1"})
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2", "--json")
    assert result.exit_code == 1
    out = json.loads(result.output)
    assert (out["result"], out["accepted"], out["phase"]) == ("refused", False, "room")
    assert out["room"]["need"]["cpu"] == 0.5
    assert out["room"]["free"]["cpu"] == 0.5
    assert d.saves == []
    text = run("set-env", "echo", "A=2")
    assert "needs 0.5 CPU twice over (1), 0.5 of 4 free" in text.output


def test_set_env_refuses_when_the_key_cannot_read_the_quota(serving, quota):
    quota["error"] = _error(403, auth.NO_VALID_SCOPE)
    d = FakeDeployment({"A": "1"})
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2")
    assert result.exit_code == 1
    assert "PROJECT scope" in result.output and "--no-room-check" in result.output
    assert d.saves == []
    result = run("set-env", "echo", "A=2", "--no-room-check", "--json")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["room"] == {
        "checked": False,
        "reason": "--no-room-check",
    }


def test_set_env_on_a_stopped_deployment_saves_and_stays_stopped(serving, quota):
    quota["error"] = AssertionError("a stopped deployment needs no room read")
    d = FakeDeployment({"A": "1"}, status="Stopped")
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2", "--json")
    assert result.exit_code == 0, result.output
    out = json.loads(result.output)
    assert out["result"] == "applied"
    assert out["after"]["status"] == "Stopped"
    assert out["room"] == {"checked": False, "reason": "not running"}


def test_set_env_refuses_while_updating(serving, quota):
    d = FakeDeployment({"A": "1"}, status="Updating")
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2")
    assert result.exit_code == 1
    assert "is Updating" in result.output
    assert d.saves == []


def test_set_env_judges_a_failed_call_by_the_read_back(serving, quota):
    """A save that raised after Hopsworks applied it reports accepted, from the versions."""
    d = FakeDeployment({"A": "1"})
    d.save_error = ConnectionError("connection reset")
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2", "--json")
    assert result.exit_code == 0, result.output
    out = json.loads(result.output)
    assert (out["result"], out["accepted"]) == ("applied", True)
    assert out["error"] == "connection reset"


def test_set_env_backend_refusal_is_proven_no_change(serving, quota):
    d = FakeDeployment({"A": "1"})
    d.apply_save = False
    d.save_error = _error(400, 240000, "Project quota exceeded")
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2", "--json")
    assert result.exit_code == 1
    out = json.loads(result.output)
    assert (out["result"], out["accepted"], out["http_status"]) == (
        "refused",
        False,
        400,
    )
    assert out["after"]["version"] == 1


def test_set_env_not_ready_names_the_state_and_the_way_back(serving, quota):
    d = FakeDeployment({"A": "1"}, outcome="Failed")
    serving.get_deployment.return_value = d
    result = run("set-env", "echo", "A=2", "--timeout", "0")
    assert result.exit_code == 1
    assert "not ready" in result.output and "Failed" in result.output
    assert "hops deployment rollback echo 1" in result.output


def test_versions_lists_keys_and_digests_not_values(serving):
    d = FakeDeployment({"A": "secret-value"})
    d.versions.append({"version": 2, "env": {}})
    d.active = 2
    serving.get_deployment.return_value = d
    result = run("versions", "echo", "--json")
    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)
    assert [(r["version"], r["active"], r["env_keys"]) for r in rows] == [
        (2, True, []),
        (1, False, ["A"]),
    ]
    assert rows[0]["env_digest"] != rows[1]["env_digest"]
    assert "secret-value" not in result.output


def test_rollback_reaches_the_version_and_reports_the_read(serving):
    d = FakeDeployment({"A": "1"})
    d.versions.append({"version": 2, "env": {"A": "2"}})
    d.active = 2
    serving.get_deployment.return_value = d
    result = run("rollback", "echo", "1", "--json")
    assert result.exit_code == 0, result.output
    assert d.rollbacks == [(1, 0)]
    out = json.loads(result.output)
    assert (out["result"], out["accepted"]) == ("applied", True)
    assert (out["before"]["version"], out["after"]["version"]) == (2, 1)

    result = run("rollback", "echo", "1")
    assert result.exit_code == 0 and result.output.startswith("Unchanged")
    result = run("rollback", "echo", "7")
    assert result.exit_code == 1 and "no version 7" in result.output
    assert d.rollbacks == [(1, 0)]


def test_predict_via_api_timing_reports_status_and_ms(serving):
    d = FakeDeployment()
    d._serving_api = mock.MagicMock()
    d._serving_api._get_hopsworks_inference_path.return_value = [
        "project",
        "119",
        "inference",
        "models",
        "echo:predict",
    ]
    serving.get_deployment.return_value = d
    rest = mock.MagicMock()
    rest._project_id = "119"
    rest._send_request.return_value = {"predictions": [[1, 2]]}
    with mock.patch("hopsworks_common.client._get_instance", return_value=rest):
        result = run(
            "predict", "echo", "--data", '{"instances": [[1, 2]]}', "--via-api",
            "--timing", "--json",
        )  # fmt: skip
        assert result.exit_code == 0, result.output
        out = json.loads(result.output)
        assert out["status"] == 200 and isinstance(out["ms"], int)
        assert out["response"] == {"predictions": [[1, 2]]}
        method, path = rest._send_request.call_args.args
        assert (method, path[-1]) == ("POST", "echo:predict")
        assert json.loads(rest._send_request.call_args.kwargs["data"]) == {
            "instances": [[1, 2]]
        }

        rest._send_request.side_effect = _error(400, None, "embedder busy")
        result = run(
            "predict", "echo", "--data", "{}", "--via-api", "--timing", "--json"
        )
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == 400
        assert out["response"] == {"errorCode": None, "usrMsg": "embedder busy"}


def test_another_project_drops_the_saved_project_id(tmp_home):
    config.save(config.HopsConfig(host="h", api_key="k", project="a", project_id=7))
    assert config.load().project_id == 7
    assert config.load(flag_project="a").project_id == 7
    assert config.load(flag_project="b").project_id is None


def _fake_connection(monkeypatch, project_info):
    import hopsworks
    from hopsworks_common import client

    rest = mock.MagicMock()
    rest._get_project_info.side_effect = project_info
    connection = mock.MagicMock()
    connection._load_serving_defaults.side_effect = _error(403, auth.NO_VALID_SCOPE)
    monkeypatch.setattr(hopsworks, "connection", lambda **kw: connection)
    monkeypatch.setattr(client, "_get_instance", lambda: rest)
    for setter in (
        "_set_kserve_installed",
        "_set_serving_num_instances_limits",
        "_set_knative_domain",
    ):
        monkeypatch.setattr(client, setter, mock.MagicMock())
    return rest, connection


def test_serving_login_takes_the_project_id_when_the_key_has_only_serving(
    monkeypatch,
):
    rest, connection = _fake_connection(monkeypatch, _error(403, auth.NO_VALID_SCOPE))
    ms = auth.login_serving("https://h", "k", "demo", project_id=119)
    assert (ms._project_name, ms._project_id) == ("demo", 119)
    assert (rest._project_name, rest._project_id) == ("demo", "119")
    assert connection._serving_defaults_loaded is True

    import click

    with pytest.raises(click.ClickException, match="HOPSWORKS_PROJECT_ID"):
        auth.login_serving("https://h", "k", "demo")


def test_serving_login_reads_the_project_by_name_when_it_may(monkeypatch):
    rest, _ = _fake_connection(
        monkeypatch, lambda name: {"projectId": 17575, "namespace": "ns"}
    )
    ms = auth.login_serving("https://h", "k", "demo", project_id=1)
    assert (ms._project_id, ms._project_namespace) == (17575, "ns")
