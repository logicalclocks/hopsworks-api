"""Per-request evaluation: the header, the switch, and the verification."""

import pytest
from fastapi.testclient import TestClient
from hopsworks_agents.protocol import AgentApp, AgentResponse
from hopsworks_agents.protocol.evaluation import (
    EvalTrial,
    RunVerifier,
    active,
    current_trial,
    in_evaluation,
    parse_baggage,
)


BAGGAGE = (
    "hopsworks.eval.run_id=run-1,hopsworks.eval.suite_id=s1,"
    "hopsworks.eval.suite_version=2,hopsworks.eval.task_id=t1,"
    "hopsworks.eval.task_version=1,hopsworks.eval.trial_id=tr1,"
    "hopsworks.eval.trial_index=3"
)


class TestParseBaggage:
    def test_reads_every_eval_field(self):
        trial = parse_baggage({"baggage": BAGGAGE})
        assert trial == EvalTrial(
            run_id="run-1",
            suite_id="s1",
            suite_version="2",
            task_id="t1",
            task_version="1",
            trial_id="tr1",
            trial_index=3,
        )

    def test_header_name_is_case_insensitive(self):
        assert parse_baggage({"Baggage": "hopsworks.eval.run_id=r"}).run_id == "r"

    def test_ignores_properties_other_members_and_encoding(self):
        raw = "  other=1 , hopsworks.eval.run_id=a%20b;prop=x ,hopsworks.eval.trial_index=nope"
        trial = parse_baggage({"baggage": raw})
        assert trial.run_id == "a b"
        # unparsable index is dropped rather than failing the request
        assert trial.trial_index is None

    def test_no_run_id_is_not_a_trial(self):
        assert parse_baggage({"baggage": "hopsworks.eval.suite_id=s1"}) is None
        assert parse_baggage({"baggage": ""}) is None
        assert parse_baggage({}) is None
        assert parse_baggage(None) is None


class TestSwitch:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv("EVAL_MODE", raising=False)
        assert not in_evaluation()
        assert current_trial() is None

    def test_environment_wins(self, monkeypatch):
        monkeypatch.setenv("EVAL_MODE", "true")
        assert in_evaluation()
        assert current_trial() is None  # an evaluation, but not a trial

    def test_active_scopes_the_trial_to_the_block(self, monkeypatch):
        monkeypatch.delenv("EVAL_MODE", raising=False)
        trial = EvalTrial(run_id="r")
        with active(trial):
            assert in_evaluation()
            assert current_trial() is trial
        assert not in_evaluation()

    def test_active_with_none_is_a_no_op(self, monkeypatch):
        monkeypatch.delenv("EVAL_MODE", raising=False)
        with active(None):
            assert not in_evaluation()


class TestRunVerifier:
    def test_accepts_a_known_run_and_remembers_it(self, monkeypatch):
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        calls = []

        def lookup(run_id):
            calls.append(run_id)
            return {"runId": run_id, "deploymentId": 7}

        verifier = RunVerifier(lookup)
        assert verifier.verify(EvalTrial(run_id="r1"))
        assert verifier.verify(EvalTrial(run_id="r1"))
        assert calls == ["r1"], "an accepted run is looked up once, not per trial"

    def test_rejects_an_unknown_run_without_remembering(self):
        calls = []

        def lookup(run_id):
            calls.append(run_id)
            return

        verifier = RunVerifier(lookup)
        assert not verifier.verify(EvalTrial(run_id="forged"))
        assert not verifier.verify(EvalTrial(run_id="forged"))
        assert calls == ["forged", "forged"], "a rejection is retried, not cached"

    def test_rejects_a_run_aimed_at_another_deployment(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "12")
        verifier = RunVerifier(lambda run_id: {"deploymentId": 99})
        assert not verifier.verify(EvalTrial(run_id="r"))

    def test_accepts_when_either_side_does_not_know_the_deployment(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "12")
        assert RunVerifier(lambda run_id: {"runId": run_id}).verify(
            EvalTrial(run_id="r")
        )
        monkeypatch.delenv("DEPLOYMENT_ID")
        assert RunVerifier(lambda run_id: {"deploymentId": 99}).verify(
            EvalTrial(run_id="r")
        )

    def test_a_lookup_failure_means_not_verified(self):
        def lookup(run_id):
            raise ConnectionError("hopsworks unreachable")

        assert not RunVerifier(lookup).verify(EvalTrial(run_id="r"))


# ── through the app ──────────────────────────────────────────────────────────


def build_app(**kwargs):
    """Echoes whether the handler and a 'tool' saw the turn as an evaluation."""
    app = AgentApp(name="eval test", **kwargs)

    @app.chat
    async def chat(request, ctx):
        # what a tool would read, with no ctx to hand
        seen_by_tool = in_evaluation()
        trial = ctx.evaluation
        return AgentResponse.text(
            text=f"tool={seen_by_tool} run={trial.run_id if trial else None}",
            conversation_id=request.conversation_id,
        )

    return app


REQUEST = {"message": {"role": "user", "content": [{"type": "text", "text": "hi"}]}}


def reply(response):
    return response.json()["message"]["content"][0]["text"]


def chat(client, headers=None):
    return client.post(
        "/v1/chat",
        json=REQUEST,
        headers=headers or {},
    )


@pytest.fixture
def no_env_eval_mode(monkeypatch):
    monkeypatch.delenv("EVAL_MODE", raising=False)


class TestManifest:
    def test_declares_per_request_when_asked(self, no_env_eval_mode):
        assert (
            TestClient(build_app())
            .get("/.well-known/hopsworks-agent.json")
            .json()["capabilities"]["eval_per_request"]
            is False
        )
        manifest = (
            TestClient(build_app(eval_per_request=True))
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert manifest["capabilities"]["eval_per_request"] is True
        assert manifest["capabilities"]["eval_mode"] is False


class TestPerRequest:
    def test_a_customer_turn_is_not_an_evaluation(self, no_env_eval_mode):
        client = TestClient(build_app(eval_per_request=True))
        response = chat(client)
        assert response.status_code == 200
        assert reply(response) == "tool=False run=None"

    def test_a_verified_trial_is(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(lambda run_id: {"runId": run_id})
        response = chat(TestClient(app), {"baggage": BAGGAGE})
        assert response.status_code == 200
        assert reply(response) == "tool=True run=run-1"

    def test_the_flag_does_not_leak_into_the_next_turn(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(lambda run_id: {"runId": run_id})
        client = TestClient(app)
        chat(client, {"baggage": BAGGAGE})
        assert reply(chat(client)) == "tool=False run=None"

    def test_an_unverifiable_trial_is_refused_not_run(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(lambda run_id: None)
        response = chat(TestClient(app), {"baggage": BAGGAGE})
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "eval_unverified"

    def test_streaming_refuses_the_same_way(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(lambda run_id: None)
        response = TestClient(app).post(
            "/v1/chat/stream",
            json=REQUEST,
            headers={"baggage": BAGGAGE},
        )
        assert response.status_code == 403

    def test_baggage_is_ignored_by_an_agent_that_did_not_declare(
        self, no_env_eval_mode
    ):
        # not declared: the agent never promised its tools check, so treating
        # the turn as an evaluation would run them for real while looking safe
        app = build_app()
        app._eval_verifier = RunVerifier(lambda run_id: {"runId": run_id})
        response = chat(TestClient(app), {"baggage": BAGGAGE})
        assert response.status_code == 200
        assert reply(response) == "tool=False run=None"

    def test_environment_wins_and_skips_verification(self, monkeypatch):
        monkeypatch.setenv("EVAL_MODE", "true")
        app = build_app()  # not even declared per-request

        def never(run_id):
            raise AssertionError("verification must not run under EVAL_MODE")

        app._eval_verifier = RunVerifier(never)
        client = TestClient(app)
        # the trial ids are still carried, for ctx.evaluation
        assert reply(chat(client, {"baggage": BAGGAGE})) == ("tool=True run=run-1")
        # and a turn with no baggage is an evaluation all the same
        assert reply(chat(client)) == "tool=True run=None"
