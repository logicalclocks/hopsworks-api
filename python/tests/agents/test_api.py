"""The scripting surface: what it sends, and what it does with a refusal."""

from __future__ import annotations

import json
import sys

import pytest
from hopsworks_agents.eval.api import (
    EvalApi,
    EvalApiError,
    evaluator,
    tool_expectation,
)


class FakeResponse:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = {} if body is None else body
        self.content = b"{}"

    def json(self):
        return self._body


class FakeSession:
    """Records what was sent, and answers with whatever was queued."""

    def __init__(self, replies=None):
        self.headers = {}
        self.verify = True
        self.sent = []
        self._replies = list(replies or [])

    def request(self, method, url, json=None, params=None, timeout=None):
        self.sent.append((method, url, json, params))
        return self._replies.pop(0) if self._replies else FakeResponse()


def api(replies=None):
    client = EvalApi.__new__(EvalApi)
    client._base = "https://h/hopsworks-api/api/project/1/agent-evals"
    client._session = FakeSession(replies)
    return client


class TestWritingAChecksConfig:
    def test_a_checks_own_configuration_is_held_apart_from_its_type(self):
        # the row shape: everything but type and name goes in config, because what a
        # check takes differs by type
        one = evaluator(
            "llm_judge", name="quality", provider="anthropic", temperature=0
        )
        assert one["type"] == "llm_judge"
        assert one["name"] == "quality"
        assert json.loads(one["config"]) == {"provider": "anthropic", "temperature": 0}

    def test_a_check_defaults_to_being_named_after_its_type(self):
        assert evaluator("tool_call")["name"] == "tool_call"

    def test_a_check_with_no_configuration_still_sends_an_object(self):
        assert json.loads(evaluator("no_tool_error")["config"]) == {}

    def test_a_tool_expectation_carries_both_directions(self):
        assert json.loads(tool_expectation(required=["a"], forbidden=["b"])) == {
            "required": ["a"],
            "forbidden": ["b"],
        }

    def test_either_direction_alone_is_enough(self):
        # a call check judges both, so a forbidden list on its own is a real expectation
        assert json.loads(tool_expectation(forbidden=["place_order"])) == {
            "required": [],
            "forbidden": ["place_order"],
        }


class TestWhatItSends:
    def test_a_suite_is_created_with_its_checks_in_the_same_call(self):
        # a suite with no checks grades by nothing and every trial comes back ungradable
        client = api()
        client.create_suite("s", evaluators=[evaluator("contains")], tags=["safety"])
        method, url, body, _ = client._session.sent[0]
        assert (method, url.endswith("/suites")) == ("POST", True)
        assert body["evaluators"][0]["type"] == "contains"
        assert json.loads(body["tags"]) == ["safety"]

    def test_adding_a_task_authors_it_then_joins_it_with_its_expectations(self):
        client = api([FakeResponse(body={"taskId": "t1"}), FakeResponse()])
        client.add_task({"suiteId": "s1", "version": 2}, "hello", {"contains": "UB40"})
        author, join = client._session.sent
        assert json.loads(author[2]["inputMessages"])[0]["content"] == "hello"
        assert "suiteId=s1&version=2" in join[1]
        assert join[2] == {"expectations": {"contains": "UB40"}}

    def test_a_task_with_no_expectations_still_sends_the_key(self):
        # an omitted map and an empty one mean different things to the server
        client = api([FakeResponse(body={"taskId": "t1"}), FakeResponse()])
        client.add_task({"suiteId": "s1", "version": 1}, "hello")
        assert client._session.sent[1][2] == {"expectations": {}}

    def test_a_run_is_started_with_query_parameters_not_a_body(self):
        # the endpoint reads the query string; a body is ignored, which would create a run
        # against no suite at all and only show up as a job failing
        client = api([FakeResponse(body={"runId": "r1"})])
        client.start_run({"suiteId": "s1", "version": 1}, deployment_id=7, n_trials=3)
        [(method, url, body, params)] = client._session.sent
        assert (method, url.endswith("/runs")) == ("POST", True)
        assert body is None
        assert params["suiteId"] == "s1"
        assert params["version"] == 1
        assert params["deploymentId"] == 7
        assert params["nTrials"] == 3

    def test_recording_and_starting_are_one_call(self):
        # a recorded run nobody started is only useful when the start itself failed
        client = api([FakeResponse(body={"runId": "r1"})])
        client.start_run({"suiteId": "s1", "version": 1}, deployment_id=1)
        assert client._session.sent[0][3]["start"] == "true"

    def test_the_runner_job_can_be_sized_before_it_exists(self):
        client = api()
        client.ensure_runner_job(
            deployment_id=7, environment_name="mine", cores=2, memory=4096
        )
        method, url, body, params = client._session.sent[0]
        assert (method, url.endswith("/runner-job")) == ("POST", True)
        assert (body["environmentName"], body["cores"]) == ("mine", 2)
        # One job per deployment, so which deployment is not optional: without it the
        # server has nothing to name the job after and nothing to attach the sizing to.
        assert params["deploymentId"] == 7

    def test_a_suite_can_state_the_gate_it_is(self):
        # tagging a suite `golden` looks like it should be enough and is not:
        # the tag replaced a category that used to imply a gate, so the gate has
        # to be stated or the suite blocks nothing while looking like it does
        client = api([FakeResponse(body={"suiteId": "s1", "version": 1})])
        client.create_suite(
            "Orders are recorded",
            evaluators=[],
            tags=["golden"],
            gate_metric="pass_rate",
            gate_threshold=1.0,
        )
        body = client._session.sent[0][2]
        assert body["gateMetric"] == "pass_rate"
        assert body["gateThreshold"] == 1.0

    def test_a_suite_that_gates_nothing_sends_no_gate(self):
        # absent rather than an empty string, so nothing downstream has to
        # decide whether "" means "no gate" or "a metric called nothing"
        client = api([FakeResponse(body={"suiteId": "s1", "version": 1})])
        client.create_suite("Catalogue answers", evaluators=[])
        body = client._session.sent[0][2]
        assert "gateMetric" not in body
        assert "gateThreshold" not in body

    def test_production_is_monitored_with_a_named_evaluator(self):
        # monitoring points a check at live traffic; it does not invent one
        client = api([FakeResponse(body={"runId": "r1", "runType": "ONLINE_SAMPLE"})])
        client.monitor_production(deployment_id=3, evaluator="tpl-1", sample=50)
        [(method, url, body, params)] = client._session.sent
        assert (method, url.endswith("/sample-runs")) == ("POST", True)
        assert body is None
        assert params["deploymentId"] == 3
        assert params["templateId"] == "tpl-1"
        assert params["sample"] == 50

    def test_a_suites_checks_can_monitor_too(self):
        # a suite without tasks is a set of checks, which is all a monitor needs
        client = api([FakeResponse(body={"runId": "r1"})])
        client.monitor_production(
            deployment_id=3, suite={"suiteId": "s1", "version": 2}
        )
        params = client._session.sent[0][3]
        assert (params["suiteId"], params["suiteVersion"]) == ("s1", 2)

    def test_monitoring_with_nothing_is_refused_before_it_is_sent(self):
        client = api()
        with pytest.raises(EvalApiError, match="evaluator or a suite"):
            client.monitor_production(deployment_id=3)
        assert client._session.sent == []

    def test_no_range_means_since_the_last_monitor(self):
        # what makes a schedule cover each trace once rather than re-grading the
        # same day every night; the server holds the watermark
        client = api([FakeResponse(body={"runId": "r1"})])
        client.monitor_production(deployment_id=3, evaluator="tpl-1")
        params = client._session.sent[0][3]
        assert "from" not in params and "to" not in params

    def test_an_explicit_range_is_sent_as_epoch_millis(self):
        # neither a browser nor a job should have to agree with the server about
        # a date format or a time zone
        from datetime import datetime, timezone

        client = api([FakeResponse(body={"runId": "r1"})])
        since = datetime(2026, 8, 1, tzinfo=timezone.utc)
        client.monitor_production(deployment_id=3, evaluator="tpl-1", since=since)
        assert client._session.sent[0][3]["from"] == int(since.timestamp() * 1000)

    def test_the_runner_job_belongs_to_a_deployment(self):
        # the sizing, environment and alerts on it are that agent's, which is the whole
        # reason the job is not shared -- so there is no project-wide call to make
        client = api()
        with pytest.raises(TypeError):
            client.ensure_runner_job(environment_name="mine")


class TestWhenTheApiRefuses:
    def test_the_reason_the_api_gave_is_what_is_raised(self):
        # the rules refuse with a sentence saying which one and why; a bare status
        # sends people to read the source instead
        client = api(
            [FakeResponse(400, {"usrMsg": "publish the suite before running it"})]
        )
        with pytest.raises(EvalApiError, match="publish the suite"):
            client.suites()

    def test_a_refusal_with_no_message_still_says_something(self):
        client = api([FakeResponse(503, {})])
        with pytest.raises(EvalApiError, match="503"):
            client.suites()

    def test_an_unparseable_refusal_does_not_mask_itself(self):
        response = FakeResponse(502)
        response.json = lambda: (_ for _ in ()).throw(ValueError("not json"))
        with pytest.raises(EvalApiError, match="502"):
            api([response]).suites()


class TestHowAJobAuthenticates:
    """A job has no API key. It has a JWT on disk, and demanding a key made the.

    runner fail on its first line inside the one place it is built to run.
    """

    def test_a_jwt_on_disk_is_used_when_there_is_no_api_key(
        self, tmp_path, monkeypatch
    ):
        from hopsworks_agents.eval.api import hopsworks_auth

        (tmp_path / "token.jwt").write_text("  the-token\n")
        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))

        request = FakeRequest()
        hopsworks_auth()(request)
        assert request.headers["Authorization"] == "Bearer the-token"

    def test_the_token_is_read_per_request_so_a_rotation_is_picked_up(
        self, tmp_path, monkeypatch
    ):
        # a suite of five hundred tasks outlives the copy read at startup
        from hopsworks_agents.eval.api import hopsworks_auth

        token = tmp_path / "token.jwt"
        token.write_text("first")
        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))

        auth = hopsworks_auth()
        first = FakeRequest()
        auth(first)
        token.write_text("second")
        second = FakeRequest()
        auth(second)

        assert first.headers["Authorization"] == "Bearer first"
        assert second.headers["Authorization"] == "Bearer second"

    def test_an_api_key_wins_when_one_is_set(self, tmp_path, monkeypatch):
        # how this works from a notebook, or anywhere outside a job
        from hopsworks_agents.eval.api import hopsworks_auth

        (tmp_path / "token.jwt").write_text("ignored")
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        monkeypatch.setenv("HOPSWORKS_API_KEY", "the-key")

        request = FakeRequest()
        hopsworks_auth()(request)
        assert request.headers["Authorization"] == "ApiKey the-key"

    def test_having_neither_says_so_rather_than_raising_a_key_error(
        self, tmp_path, monkeypatch
    ):
        # KeyError: 'HOPSWORKS_API_KEY' is a stack trace, not an explanation
        from hopsworks_agents.eval.api import hopsworks_auth

        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        with pytest.raises(EvalApiError, match="nothing to authenticate with"):
            hopsworks_auth()


class FakeRequest:
    def __init__(self):
        self.headers = {}


class TestReachingTheApiFromInsideAJob:
    """Auth and the cluster's CA chain both come from the client that is already.

    connected. Rebuilding either by hand failed twice: a job has no API key, and
    the internal endpoint is signed by a CA no system trust store carries.
    """

    def test_the_connected_clients_auth_and_ca_chain_are_used(self, monkeypatch):
        from hopsworks_agents.eval import api as api_module

        monkeypatch.setattr(
            api_module,
            "_from_hopsworks_client",
            lambda: ("the-auth", "/srv/hops/certs/ca_chain.pem"),
        )
        session = api_module.hopsworks_session()
        assert session.auth == "the-auth"
        assert session.verify == "/srv/hops/certs/ca_chain.pem"

    def test_it_falls_back_when_no_client_is_connected(self, tmp_path, monkeypatch):
        # a script or a notebook, where there is an API key and a public endpoint
        from hopsworks_agents.eval import api as api_module

        monkeypatch.setattr(api_module, "_from_hopsworks_client", lambda: None)
        monkeypatch.setenv("HOPSWORKS_API_KEY", "the-key")
        request = FakeRequest()
        api_module.hopsworks_session().auth(request)
        assert request.headers["Authorization"] == "ApiKey the-key"

    def test_the_accessor_this_calls_is_the_one_the_client_has(self):
        """The test that was missing.

        The previous version asserted only that _from_hopsworks_client() returns
        None with nothing connected — which it did, but because the accessor name
        was wrong, not because no client was there. The runner then fell back to
        the system trust store and failed TLS against the cluster's certificate,
        twice, with a green suite.
        """
        client = pytest.importorskip(
            "hopsworks_common.client",
            reason="the hopsworks client is only installed in the job image",
        )
        assert hasattr(client, "_get_instance") or hasattr(client, "get_instance")

    def test_a_client_that_is_not_connected_falls_back(self, monkeypatch):
        from hopsworks_agents.eval import api as api_module

        module = type(sys)("hopsworks_common.client")

        def _get_instance():
            raise RuntimeError("not connected")

        module._get_instance = _get_instance
        monkeypatch.setitem(sys.modules, "hopsworks_common.client", module)
        monkeypatch.setitem(
            sys.modules, "hopsworks_common", type(sys)("hopsworks_common")
        )
        sys.modules["hopsworks_common"].client = module
        assert api_module._from_hopsworks_client() is None

    def test_a_connected_client_supplies_both_halves(self, monkeypatch):
        from hopsworks_agents.eval import api as api_module

        module = type(sys)("hopsworks_common.client")
        module._get_instance = lambda: type(
            "C", (), {"_auth": "auth-object", "_verify": "/ca_chain.pem"}
        )()
        monkeypatch.setitem(sys.modules, "hopsworks_common.client", module)
        monkeypatch.setitem(
            sys.modules, "hopsworks_common", type(sys)("hopsworks_common")
        )
        sys.modules["hopsworks_common"].client = module
        assert api_module._from_hopsworks_client() == ("auth-object", "/ca_chain.pem")


class TestTheEvaluatorLibrary:
    """Named sets of checks, so a judge's criteria are written once."""

    def test_saving_one_sends_the_checks_as_a_spec(self):
        client = api()
        client.save_evaluator("Account untouched", [{"type": "tool_call"}], "why")
        method, url, body, _ = client._session.sent[0]
        assert (method, url.endswith("/evaluators")) == ("POST", True)
        assert json.loads(body["spec"]) == [{"type": "tool_call"}]
        assert body["description"] == "why"

    def test_a_saved_entry_is_a_copy_source_not_a_reference(self):
        # a suite copies these in when created; the library changing later must
        # not alter what a published suite means
        client = api([FakeResponse(body=[{"name": "Account untouched"}])])
        assert client.evaluators()[0]["name"] == "Account untouched"
