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

    def test_a_percent_encoded_key_is_the_same_key(self):
        """The propagator decodes keys, so this has to as well.

        It did not, and took the first of several headers besides. The spans
        are stamped by the propagator, so
        `run_id=LIVE,hopsworks%2Eeval%2Erun_id=OTHER` was verified as LIVE and
        recorded as OTHER -- a way to label a turn as a trial of a run it has
        nothing to do with, knowing only one live run of the deployment.
        """
        raw = "hopsworks.eval.run_id=LIVE,hopsworks%2Eeval%2Erun_id=OTHER"
        # last one wins, as it does in the propagator
        assert parse_baggage({"baggage": raw}).run_id == "OTHER"

    def test_every_baggage_header_is_read(self):
        class Headers(dict):
            def getlist(self, name):
                return [v for k, v in self.items() if k.lower() == name]

        headers = Headers({"baggage": "hopsworks.eval.run_id=first"})
        assert parse_baggage(headers).run_id == "first"

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

    def test_accepts_when_this_process_does_not_know_its_own_deployment(
        self, monkeypatch
    ):
        # local development: nothing to compare against, and no production
        # traffic to confuse either
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        assert RunVerifier(lambda run_id: {"deploymentId": 99}).verify(
            EvalTrial(run_id="r")
        )

    def test_rejects_a_run_that_names_no_deployment(self, monkeypatch):
        # Every run row Hopsworks writes names one. A reply that does not is not
        # a run this agent can check itself against, and "cannot check" must not
        # read as "checked".
        monkeypatch.setenv("DEPLOYMENT_ID", "12")
        assert not RunVerifier(
            lambda run_id: {"runId": run_id, "deploymentId": 7, "status": "RUNNING"}
        ).verify(EvalTrial(run_id="r"))

    def test_rejects_a_run_that_is_over(self, monkeypatch):
        # Run ids are listed to every project member and stamped on trace
        # attributes. Replaying a finished one made a real customer turn skip the
        # write tools and report success without having done anything.
        monkeypatch.setenv("DEPLOYMENT_ID", "7")
        verifier = RunVerifier(
            lambda run_id: {"deploymentId": 7, "status": "SUCCEEDED"}
        )
        assert not verifier.verify(EvalTrial(run_id="old"))

    def test_an_acceptance_does_not_stand_for_ever(self, monkeypatch):
        # One lookup per suite rather than one per task -- but a run that ends
        # while the process lives stops being a licence.
        monkeypatch.setenv("DEPLOYMENT_ID", "7")
        clock = [0.0]
        runs = [{"deploymentId": 7, "status": "RUNNING"}]
        verifier = RunVerifier(lambda run_id: runs[0], now=lambda: clock[0])
        assert verifier.verify(EvalTrial(run_id="r1"))
        runs[0] = {"deploymentId": 7, "status": "CANCELLED"}
        assert verifier.verify(EvalTrial(run_id="r1")), "still inside the window"
        clock[0] += 61.0
        assert not verifier.verify(EvalTrial(run_id="r1"))

    def test_a_deployments_own_api_key_file_authenticates_the_lookup(
        self, tmp_path, monkeypatch
    ):
        # A serving pod has /keys/api.key and no JWT, so the lookup raised
        # "no HOPSWORKS_API_KEY and no token.jwt in SECRETS_DIR" and the agent
        # answered 403 to every evaluation request it was sent.
        from hopsworks_agents.protocol.evaluation import _authorization

        (tmp_path / "api.key").write_text("  serving-key\n")
        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        assert _authorization() == "ApiKey serving-key"

    def test_a_jwt_still_wins_over_a_key_file(self, tmp_path, monkeypatch):
        from hopsworks_agents.protocol.evaluation import _authorization

        (tmp_path / "token.jwt").write_text("the-token")
        (tmp_path / "api.key").write_text("the-key")
        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        assert _authorization() == "Bearer the-token"

    def test_having_no_credential_at_all_names_the_three_it_looked_for(
        self, tmp_path, monkeypatch
    ):
        from hopsworks_agents.protocol.evaluation import _authorization

        monkeypatch.delenv("HOPSWORKS_API_KEY", raising=False)
        monkeypatch.setenv("SECRETS_DIR", str(tmp_path))
        with pytest.raises(RuntimeError, match="neither token.jwt nor api.key"):
            _authorization()

    def test_a_named_ca_bundle_is_trusted_for_the_lookup(self, tmp_path, monkeypatch):
        # The internal endpoint is signed by the cluster's own CA, so the lookup
        # failed TLS verification -- after the credential was found -- and the
        # agent still answered 403 to every evaluation request.
        from hopsworks_agents.protocol import evaluation

        bundle = tmp_path / "bundle.pem"
        bundle.write_text("not parsed here")
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))
        assert evaluation._ca_chain() == str(bundle)

    def test_no_truststore_leaves_the_system_default_alone(self, tmp_path, monkeypatch):
        from hopsworks_agents.protocol import evaluation

        monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
        monkeypatch.delenv("SSL_CERT_FILE", raising=False)
        monkeypatch.delenv("MATERIAL_DIRECTORY", raising=False)
        monkeypatch.setattr(evaluation, "_CA_CHAIN_PATH", str(tmp_path / "missing.pem"))
        assert evaluation._ca_chain() is None
        assert evaluation._tls_context() is None

    def test_hostname_verification_can_be_turned_off_as_the_client_allows(
        self, monkeypatch
    ):
        # A cluster whose certificate does not carry the name its endpoint is
        # reached by; the platform client honours the same switch.
        import ssl

        from hopsworks_agents.protocol import evaluation

        system = ssl.get_default_verify_paths().cafile
        if not system:
            pytest.skip("no system CA bundle to point at")
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", system)
        monkeypatch.setenv("HOPSWORKS_HOSTNAME_VERIFICATION", "false")
        context = evaluation._tls_context()
        assert context is not None and not context.check_hostname

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
            .json()["capabilities"]["custom"]["hopsworks"]["eval_per_request"]
            is False
        )
        manifest = (
            TestClient(build_app(eval_per_request=True))
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert (
            manifest["capabilities"]["custom"]["hopsworks"]["eval_per_request"] is True
        )
        assert manifest["capabilities"]["custom"]["hopsworks"]["eval_mode"] is False


class TestPerRequest:
    def test_a_customer_turn_is_not_an_evaluation(self, no_env_eval_mode):
        client = TestClient(build_app(eval_per_request=True))
        response = chat(client)
        assert response.status_code == 200
        assert reply(response) == "tool=False run=None"

    def test_a_verified_trial_is(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(
            lambda run_id: {"runId": run_id, "deploymentId": 7, "status": "RUNNING"}
        )
        response = chat(TestClient(app), {"baggage": BAGGAGE})
        assert response.status_code == 200
        assert reply(response) == "tool=True run=run-1"

    def test_the_flag_does_not_leak_into_the_next_turn(self, no_env_eval_mode):
        app = build_app(eval_per_request=True)
        app._eval_verifier = RunVerifier(
            lambda run_id: {"runId": run_id, "deploymentId": 7, "status": "RUNNING"}
        )
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
            "/v1/run",
            json={
                "threadId": "",
                "runId": "r1",
                "messages": [{"id": "m1", "role": "user", "content": "hi"}],
            },
            headers={"baggage": BAGGAGE},
        )
        assert response.status_code == 403

    def test_baggage_is_ignored_by_an_agent_that_did_not_declare(
        self, no_env_eval_mode
    ):
        # not declared: the agent never promised its tools check, so treating
        # the turn as an evaluation would run them for real while looking safe
        app = build_app()
        app._eval_verifier = RunVerifier(
            lambda run_id: {"runId": run_id, "deploymentId": 7, "status": "RUNNING"}
        )
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
