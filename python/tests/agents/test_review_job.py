import json

import pytest
from hopsworks_agents.eval import review_job as rj


class FakeResponse:
    def __init__(self, body, status=200):
        self._body = body
        self.status = status
        # the real client reads requests' attribute name
        self.status_code = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self._body


class FakeSession:
    """Routes the handful of endpoints the review job touches."""

    def __init__(self, feedback, details=None, sessions=None, run=None, job=None):
        self.feedback = feedback
        self.details = details or {}
        self.sessions = sessions or {}
        self.run = run
        self.job = job
        self.puts = []
        self.feedback_requests = []

    def get(self, url, params=None, timeout=None):
        if url.endswith("/feedback/clusters"):
            return FakeResponse(getattr(self, "clusters", []))
        if url.endswith("/feedback"):
            self.feedback_requests.append(dict(params or {}))
            offset = int(params.get("offset", 0))
            limit = int(params.get("limit", 100))
            rows = self.feedback
            if params.get("traceId"):
                rows = [r for r in rows if r["traceId"] == params["traceId"]]
            return FakeResponse(
                {"count": len(rows), "items": rows[offset : offset + limit]}
            )
        if "/traces/sessions/" in url:
            return FakeResponse(
                {"items": self.sessions.get(url.rsplit("/", 1)[-1], [])}
            )
        if "/traces/" in url:
            trace_id = url.rsplit("/", 1)[-1]
            if trace_id not in self.details:
                raise RuntimeError("no such trace")
            return FakeResponse(self.details[trace_id])
        if "/runs/" in url:
            return FakeResponse(self.run)
        if "/jobs/" in url:
            return FakeResponse(self.job)
        if "/serving/" in url:
            if getattr(self, "serving", None) is None:
                return FakeResponse({"errorMsg": "no"}, status=500)
            return FakeResponse(self.serving)
        raise AssertionError(url)

    def put(self, url, params=None, timeout=None):
        self.puts.append((url, dict(params or {})))
        return FakeResponse({})


class FakeClient:
    def __init__(self, traces=None):
        self.traces = traces or {}

    def fetch_trace(self, trace_id):
        return self.traces.get(trace_id)


class FakeGroup:
    def __init__(self):
        self.features = []
        self.frames = []

    def insert(self, frame, write_options=None):
        self.frames.append(frame)


class FakeFeatureStore:
    def __init__(self):
        self.group = FakeGroup()
        self.clusters = FakeGroup()
        self.asked = []

    def get_feature_group(self, name, version):
        self.asked.append((name, version))
        return self.clusters if name == rj.CLUSTERS_FG else self.group


def feedback(
    i, *, created="2026-09-10T08:00:0{}Z", verdict="negative", trace="trace-{}"
):
    return {
        "feedbackId": f"fb-{i}",
        "deploymentId": 3,
        "traceId": trace.format(i),
        "sessionId": "sess-1",
        "verdict": verdict,
        "issueCategory": "wrong_answer",
        "correctedAnswer": "it should be 44.10",
        "createdAt": created.format(i),
    }


def detail(question="What is my total?", answer="Your total is $49."):
    return {
        "spans": [
            {
                "startTimeNs": 5_000,
                "messages": json.dumps(
                    [
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": answer},
                    ]
                ),
            }
        ]
    }


def good_reply(_prompt):
    return json.dumps(
        {
            "category": "wrong_answer",
            "severity": "high",
            "failure_summary": "Quotes the pre-discount total.",
            "failure_signature": "discount not applied",
            "correction_status": "usable",
            "correction_grounding": "consistent_with_tools",
            "normalized_correction": "Your total is $44.10.",
            "proposed_rubric": "Applies the discount.",
            "proposed_assertions": [{"kind": "contains", "value": "44.10"}],
            "redaction_findings": [],
            "needs_human": False,
            "confidence": 0.9,
        }
    )


def run_row(**overrides):
    row = {
        "runId": "run-1",
        "runType": "FEEDBACK_REVIEW",
        "deploymentId": 3,
        "nTrials": 200,
        "sampleFrom": "2026-09-09T08:00:00Z",
        "sampleTo": "2026-09-10T09:00:00Z",
        "jobName": "agent_feedback_review",
    }
    row.update(overrides)
    return row


SETTINGS = {
    "provider": "anthropic",
    "model": "claude-sonnet-5",
    "reasoning_effort": "",
    "api_key_env": "",
    "context_turns": 20,
}


class TestSettings:
    def test_come_off_the_jobs_configuration_with_the_backends_defaults(self):
        settings = rj.review_settings(
            {
                "config": {
                    "provider": "openai",
                    "model": "gpt-5",
                    "reasoningEffort": "low",
                    "apiKeyEnv": "MY_KEY",
                    "contextTurns": 6,
                }
            }
        )
        assert {
            k: settings[k]
            for k in (
                "provider",
                "model",
                "reasoning_effort",
                "api_key_env",
                "context_turns",
            )
        } == {
            "provider": "openai",
            "model": "gpt-5",
            "reasoning_effort": "low",
            "api_key_env": "MY_KEY",
            "context_turns": 6,
        }
        assert rj.review_settings(None)["provider"] == "anthropic"
        assert rj.review_settings({})["context_turns"] == 20

    def test_sources_and_code_reading_have_defaults_and_can_be_set(self):
        defaults = rj.review_settings({})
        assert defaults["sources"] == ("feedback", "errors", "judge")
        assert (
            defaults["read_source_code"] is True and defaults["source_location"] == ""
        )
        custom = rj.review_settings(
            {
                "config": {
                    "sources": "feedback, anomalies",
                    "readSourceCode": False,
                    "sourceLocation": "https://github.com/o/r#main",
                }
            }
        )
        assert custom["sources"] == ("feedback", "anomalies")
        assert custom["read_source_code"] is False
        assert custom["source_location"] == "https://github.com/o/r#main"

    def test_the_code_location_is_the_deployments_unless_the_job_overrides_it(self):
        session = FakeSession([])
        session.serving = {
            "gitUrl": "https://github.com/o/r",
            "gitBranch": "main",
            "gitCurrentCommit": "abc",
            "predictor": "chinook/agent.py",
        }
        described = rj.code_location(session, "http://h", 1, 3, rj.review_settings({}))
        assert (
            described.git_url == "https://github.com/o/r"
            and described.git_commit == "abc"
        )
        assert described.script_file == "chinook/agent.py"
        overridden = rj.code_location(
            session,
            "http://h",
            1,
            3,
            rj.review_settings(
                {"config": {"sourceLocation": "/Projects/p/Models/agent/2"}}
            ),
        )
        assert overridden.model_path == "/Projects/p/Models/agent/2"
        # the entry script is still the deployment's
        assert overridden.script_file == "chinook/agent.py"
        unreadable = rj.code_location(
            FakeSession([]), "http://h", 1, 3, rj.review_settings({})
        )
        assert not unreadable.known()

    def test_a_custom_endpoint_carries_its_base_url_and_headers(self, monkeypatch):
        settings = rj.review_settings(
            {
                "config": {
                    "provider": "custom",
                    "model": "local",
                    "baseUrl": "https://gw.internal/v1",
                    "headers": '{"X-Tenant": "acme"}',
                }
            }
        )
        assert settings["base_url"] == "https://gw.internal/v1"
        assert settings["headers"] == {"X-Tenant": "acme"}
        assert rj.review_settings({"config": {"headers": "not json"}})["headers"] == {}
        # an OpenAI-compatible provider without a base url is a reason, not a crash
        monkeypatch.setenv("MY_KEY", "k")
        complete, why = rj.completer_from(
            {**settings, "base_url": "", "api_key_env": "MY_KEY"}
        )
        assert complete is None and "base URL" in why

    def test_a_missing_key_is_a_reason_not_an_exception(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        complete, why = rj.completer_from(SETTINGS)
        assert complete is None
        assert "ANTHROPIC_API_KEY" in why


class TestTheWindow:
    def test_asks_the_server_for_the_run_window_and_walks_every_page(self):
        session = FakeSession([feedback(i) for i in range(7)])
        rj.PAGE, saved = 3, rj.PAGE
        try:
            rows = rj.feedback_in_window(session, "http://h/otel", 1_000.0, 2_000.0)
        finally:
            rj.PAGE = saved
        assert len(rows) == 7
        assert len(session.feedback_requests) == 3
        first = session.feedback_requests[0]
        # everything that is not an endorsement, bounded by when it was said
        assert (
            first["verdict"] == "negative"
            and first["from"] == "1000"
            and first["to"] == "2000"
        )

    def test_one_trace_is_asked_for_by_id_whenever_it_was_given(self):
        session = FakeSession([feedback(0), feedback(1)])
        rows = rj.feedback_in_window(
            session, "http://h/otel", 1_000.0, 2_000.0, trace_id="trace-1"
        )
        assert [r["feedbackId"] for r in rows] == ["fb-1"]
        request = session.feedback_requests[0]
        assert request["traceId"] == "trace-1"
        assert "from" not in request and "to" not in request

    def test_the_run_row_says_which_trace_it_is_about(self):
        assert rj.trace_of({"sampleSource": "feedback:trace:305b97bb"}) == "305b97bb"
        assert rj.trace_of({"sampleSource": "feedback:window"}) == ""
        assert rj.trace_of({"sampleSource": "feedback"}) == ""
        assert rj.trace_of({}) == ""

    def test_oldest_first_whatever_order_the_server_used(self):
        session = FakeSession([feedback(2), feedback(0), feedback(1)])
        rows = rj.feedback_in_window(session, "http://h/otel", 0, 10**13)
        assert [r["feedbackId"] for r in rows] == ["fb-0", "fb-1", "fb-2"]


class TestReviewing:
    def test_existing_signatures_are_offered_to_the_model(self):
        from hopsworks_agents.eval.clusters import Cluster

        prompts = []

        def complete(prompt):
            prompts.append(prompt)
            return good_reply(prompt)

        session = FakeSession([feedback(0)], details={"trace-0": detail()})
        rj.review_feedback(
            session,
            FakeClient(),
            "http://h/otel",
            run_row(),
            SETTINGS,
            complete,
            clusters=[
                Cluster(
                    cluster_id="c",
                    deployment_id=3,
                    signature="customer key not used",
                    size=9,
                )
            ],
        )
        assert "Existing failure signatures for this agent" in prompts[0]
        assert "- customer key not used" in prompts[0]

    def test_each_verdict_becomes_a_row_with_the_conversation_and_tools_in_view(self):
        prompts = []

        def complete(prompt):
            prompts.append(prompt)
            return good_reply(prompt)

        session = FakeSession(
            [feedback(0)],
            details={"trace-0": detail()},
            sessions={
                "sess-1": [
                    {
                        "startTimeNs": 1_000,
                        "messages": json.dumps(
                            [
                                {"role": "user", "content": "I have a discount code"},
                                {"role": "assistant", "content": "Applied"},
                            ]
                        ),
                    }
                ]
            },
        )
        client = FakeClient(
            {
                "trace-0": {
                    "tool_calls": [
                        {
                            "name": "get_cart_total",
                            "arguments": "{}",
                            "result": "44.10",
                            "status": "OK",
                        }
                    ]
                }
            }
        )
        rows, processed = rj.review_feedback(
            session, client, "http://h/otel", run_row(), SETTINGS, complete
        )
        assert processed is None
        assert len(rows) == 1 and rows[0]["ungradable"] is False
        assert rows[0]["category"] == "wrong_answer" and rows[0]["run_id"] == "run-1"
        assert (
            rows[0]["provider"] == "anthropic" and rows[0]["model"] == "claude-sonnet-5"
        )
        assert "I have a discount code" in prompts[0]
        assert "get_cart_total" in prompts[0] and "44.10" in prompts[0]

    def test_the_agents_code_is_shown_for_the_tools_the_trace_called(self):
        from hopsworks_agents.eval.agent_source import SourceBundle

        prompts = []

        def complete(prompt):
            prompts.append(prompt)
            return good_reply(prompt)

        source = SourceBundle(
            files={
                "agent.py": "from tools import get_cart_total\n",
                "tools.py": "def get_cart_total():\n    return 49\n",
                "billing.py": "def discount(): ...\n",
            },
            entry="agent.py",
            origin="git https://x/y@abc",
        )
        session = FakeSession([feedback(0)], details={"trace-0": detail()})
        client = FakeClient(
            {
                "trace-0": {
                    "tool_calls": [
                        {
                            "name": "get_cart_total",
                            "arguments": "{}",
                            "result": "49",
                            "status": "OK",
                        }
                    ],
                    "tool_names": ["get_cart_total"],
                }
            }
        )
        rj.review_feedback(
            session,
            client,
            "http://h/otel",
            run_row(),
            SETTINGS,
            complete,
            source=source,
        )
        assert '<file path="agent.py">' in prompts[0]
        assert '<file path="tools.py">' in prompts[0]
        assert "billing.py" not in prompts[0]
        assert 'origin="git https://x/y@abc"' in prompts[0]

    def test_the_budget_cuts_the_window_and_says_where_it_stopped(self):
        session = FakeSession(
            [feedback(i) for i in range(5)],
            details={f"trace-{i}": detail() for i in range(5)},
        )
        rows, processed = rj.review_feedback(
            session,
            FakeClient(),
            "http://h/otel",
            run_row(nTrials=2),
            SETTINGS,
            good_reply,
        )
        assert [r["feedback_id"] for r in rows] == ["fb-0", "fb-1"]
        # the second row's timestamp: the next run starts there, not at the end of the window
        assert processed == rj._ms("2026-09-10T08:00:01Z")

    def test_no_model_means_every_row_says_so_rather_than_a_silent_success(self):
        session = FakeSession([feedback(0), feedback(1)])
        rows, _ = rj.review_feedback(
            session,
            FakeClient(),
            "http://h/otel",
            run_row(),
            SETTINGS,
            None,
            no_model_reason="no API key: set ANTHROPIC_API_KEY",
        )
        assert all(r["ungradable"] for r in rows)
        assert rows[0]["error"].startswith("no API key")
        assert all(r["needs_human"] for r in rows)

    def test_an_unreadable_trace_is_one_row_not_a_failed_run(self):
        session = FakeSession([feedback(0), feedback(1)], details={"trace-1": detail()})
        rows, _ = rj.review_feedback(
            session, FakeClient(), "http://h/otel", run_row(), SETTINGS, good_reply
        )
        assert (
            rows[0]["ungradable"] is True and "could not read trace" in rows[0]["error"]
        )
        assert rows[1]["ungradable"] is False

    def test_an_empty_window_is_nothing_to_do(self):
        rows, processed = rj.review_feedback(
            FakeSession([]),
            FakeClient(),
            "http://h/otel",
            run_row(),
            SETTINGS,
            good_reply,
        )
        assert rows == [] and processed is None


class TestWriting:
    def test_rows_go_to_the_triage_group_through_the_schema_matcher(self):
        pytest.importorskip("pandas")
        store = FakeFeatureStore()
        rj.write_triage(
            store,
            [
                rj.triage_row(
                    feedback(0), None, run_id="r", provider="p", model="m", error="x"
                )
            ],
        )
        assert store.asked == [(rj.TRIAGE_FG, 1)]
        assert len(store.group.frames) == 1 and list(
            store.group.frames[0]["feedback_id"]
        ) == ["fb-0"]

    def test_clusters_are_written_beside_the_triage(self):
        pytest.importorskip("pandas")
        from hopsworks_agents.eval.clusters import Cluster

        store = FakeFeatureStore()
        rj.write_clusters(
            store, [Cluster(cluster_id="c1", deployment_id=3, signature="s", size=2)]
        )
        assert (rj.CLUSTERS_FG, 1) in store.asked
        frame = store.clusters.frames[0]
        assert list(frame["cluster_id"]) == ["c1"] and list(frame["size"]) == [2]
        assert frame["first_seen"].notna().all() and frame["decided_at"].iloc[0] == ""

    def test_a_missing_group_is_named_not_an_attribute_error(self):
        pytest.importorskip("pandas")

        class NoGroup(FakeFeatureStore):
            def get_feature_group(self, name, version):
                return None

        with pytest.raises(
            RuntimeError, match="agent_feedback_triage v1 does not exist"
        ):
            rj.write_triage(
                NoGroup(),
                [rj.triage_row(feedback(0), None, run_id="r", provider="p", model="m")],
            )

    def test_an_undecided_timestamp_column_is_typed_not_null(self):
        # decided_at is None on every fresh row; left as object it reaches Delta as a Null type
        # and the whole insert is refused after the model calls were paid for
        pd = pytest.importorskip("pandas")
        from hopsworks_agents.eval.run_job import _match_schema

        class Feature:
            def __init__(self, name, type_):
                self.name, self.type = name, type_

        group = FakeGroup()
        group.features = [
            Feature("created_at", "timestamp"),
            Feature("other_at", "timestamp"),
        ]
        rows = [
            rj.triage_row(feedback(i), None, run_id="r", provider="p", model="m")
            for i in range(2)
        ]
        frame = pd.DataFrame(rows)
        frame["other_at"] = None
        frame = _match_schema(group, frame)
        assert str(frame["other_at"].dtype) == "datetime64[us, UTC]"
        assert frame["other_at"].isna().all()
        assert str(frame["created_at"].dtype) == "datetime64[us, UTC]"
        assert frame["created_at"].notna().all()
        # and the one column that is empty until a person acts is text, never a null timestamp
        assert list(frame["decided_at"]) == ["", ""]

    def test_nothing_is_written_for_an_empty_run(self):
        store = FakeFeatureStore()
        rj.write_triage(store, [])
        assert store.asked == []


class TestOneExecution:
    class Project:
        id = 9
        name = "demo"

        def __init__(self, store):
            self._store = store

        def get_feature_store(self):
            return self._store

    def test_reports_success_with_where_it_stopped(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
        monkeypatch.setattr(rj, "completer_for", lambda config, key: good_reply)
        session = FakeSession(
            [feedback(i) for i in range(3)],
            details={f"trace-{i}": detail() for i in range(3)},
            run=run_row(nTrials=2),
            job={"config": {"provider": "anthropic", "model": "m"}},
        )
        store = FakeFeatureStore()
        assert (
            rj._execute(
                "run-1",
                session,
                "http://h/agent-evals",
                self.Project(store),
                "http://h",
            )
            is True
        )
        url, params = session.puts[-1]
        assert url.endswith("/runs/run-1/status") and params["status"] == "SUCCEEDED"
        assert params["processedThrough"] == str(int(rj._ms("2026-09-10T08:00:01Z")))
        assert len(store.group.frames) == 1
        # both verdicts shared a signature, so one cluster of two, and the rows point at it
        assert len(store.clusters.frames) == 1
        clusters = store.clusters.frames[0]
        assert list(clusters["size"]) == [2] and list(clusters["signature"]) == [
            "discount not applied"
        ]
        assert set(store.group.frames[0]["cluster_id"]) == {
            clusters["cluster_id"].iloc[0]
        }

    def test_a_run_of_the_wrong_type_is_refused_and_reported(self):
        session = FakeSession([], run=run_row(runType="SUITE"))
        assert (
            rj._execute(
                "run-1",
                session,
                "http://h/agent-evals",
                self.Project(FakeFeatureStore()),
                "http://h",
            )
            is False
        )
        assert session.puts[-1][1]["status"] == "FAILED"

    def test_a_crash_is_reported_as_failed_not_left_running(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "k")
        monkeypatch.setattr(rj, "completer_for", lambda config, key: good_reply)

        class Broken(FakeFeatureStore):
            def get_feature_group(self, name, version):
                raise RuntimeError("no such feature group")

        session = FakeSession(
            [feedback(0)], details={"trace-0": detail()}, run=run_row(), job={}
        )
        assert (
            rj._execute(
                "run-1",
                session,
                "http://h/agent-evals",
                self.Project(Broken()),
                "http://h",
            )
            is False
        )
        url, params = session.puts[-1]
        assert (
            params["status"] == "FAILED"
            and "no such feature group" in params["errorMessage"]
        )


class TestArguments:
    def test_the_schedulers_start_time_is_tolerated(self, monkeypatch):
        import sys

        seen = {}
        monkeypatch.setattr(
            rj,
            "_execute",
            lambda run_id, *a, **k: seen.setdefault("run", run_id) or True,
        )
        monkeypatch.setattr(rj, "hopsworks_session", lambda: object())
        monkeypatch.setenv("HOPSWORKS_HOST", "https://h")
        fake_hopsworks = type(
            "H",
            (),
            {"login": staticmethod(lambda: type("P", (), {"id": 1, "name": "p"})())},
        )
        monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)
        monkeypatch.setattr(
            sys,
            "argv",
            ["review", "--run-id", "r1", "-start_time", "2026-09-16T08:00:00Z"],
        )
        rj.main()
        assert seen["run"] == "r1"


class TestTheBudgetField:
    def test_is_read_however_the_server_spelled_it(self):
        from hopsworks_agents.eval.run_fields import run_trials

        assert run_trials({"nTrials": 7}, 0) == 7
        assert run_trials({"ntrials": 7}, 0) == 7
        assert run_trials({}, 3) == 3
        assert run_trials({"nTrials": "x"}, 3) == 3
