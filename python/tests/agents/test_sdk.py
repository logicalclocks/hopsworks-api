"""The client: what each call sends, and what comes back as objects."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from hopsworks_agents.eval.sdk import (
    Agent,
    AgentServing,
    AgentServingError,
    ChatStream,
    Cluster,
    Suite,
    Task,
    check,
)
from hopsworks_agents.eval.sdk.models import (
    EvaluatorTemplate,
    Feedback,
    Run,
    as_datetime,
)


class Reply:
    def __init__(self, body=None, status=200, lines=None, content=None, headers=None):
        self._body = body
        self._lines = lines
        self.status_code = status
        self.headers = headers or {}
        if content is not None:
            self.content = content
        else:
            self.content = b"" if body is None and lines is None else b"x"

    def json(self):
        return self._body

    def iter_lines(self, decode_unicode=False):
        return iter(self._lines or [])


class FakeSession:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.sent: list[tuple[str, str, dict | None, object]] = []

    def request(
        self,
        method,
        url,
        params=None,
        json=None,
        data=None,
        headers=None,
        timeout=None,
        stream=False,
    ):
        self.streamed = stream
        self.sent.append(
            (
                method,
                url.replace("https://h/hopsworks-api/api/project/1", ""),
                params,
                json if data is None else data,
            )
        )
        self.headers = headers
        return self.replies.pop(0) if self.replies else Reply({})


def client(*replies):
    session = FakeSession(*replies)
    return (
        AgentServing("https://h", 1, session=session, gateway_url="https://gw"),
        session,
    )


SERVING = {
    "id": 7,
    "name": "support",
    "projectNamespace": "g1",
    "modelServer": "PYTHON",
}


def agent(*replies) -> tuple[AgentServing, Agent, FakeSession]:
    """A client whose agent 7 is already fetched; only the test's own requests are recorded."""
    evals, session = client(Reply(SERVING), *replies)
    fetched = evals.get_agent(7)
    session.sent.clear()
    return evals, fetched, session


class TestTransport:
    def test_a_refusal_carries_the_apis_own_message_and_status(self):
        evals, _ = client(
            Reply({"usrMsg": "a suite needs a name", "errorCode": 1}, status=400)
        )
        with pytest.raises(AgentServingError, match="a suite needs a name") as err:
            evals.suites.list()
        assert err.value.status == 400

    def test_an_agents_own_refusal_keeps_its_code_and_message(self):
        # A handler's AgentError arrives as FastAPI's `detail`, which for this
        # protocol is {"code", "message", "retryable"}. Only the platform's
        # usrMsg/errorMsg were read, so the one refusal the protocol defines an
        # envelope for reached the caller as a bare "HTTP 409" -- no message to
        # show a person and no code to branch on.
        evals, _ = client(
            Reply(
                {
                    "detail": {
                        "code": "contract_refusal",
                        "message": "the agent refused this turn on purpose",
                        "retryable": False,
                    }
                },
                status=409,
            )
        )
        with pytest.raises(AgentServingError) as err:
            evals.suites.list()

        assert err.value.status == 409
        assert err.value.code == "contract_refusal"
        assert err.value.retryable is False
        assert "the agent refused this turn on purpose" in str(err.value)
        assert "contract_refusal" in str(err.value)

    def test_a_retryable_refusal_says_so(self):
        evals, _ = client(
            Reply(
                {"detail": {"code": "vector_purge_failed", "retryable": True}},
                status=500,
            )
        )
        with pytest.raises(AgentServingError) as err:
            evals.suites.list()
        assert err.value.retryable is True
        # no message in the envelope, so the status is all there is to say
        assert "HTTP 500" in str(err.value)

    def test_a_plain_string_detail_is_the_message(self):
        # FastAPI's own refusals -- request validation, a missing route -- put a
        # string here rather than the protocol's object.
        evals, _ = client(Reply({"detail": "Not Found"}, status=404))
        with pytest.raises(AgentServingError, match="Not Found") as err:
            evals.suites.list()
        assert err.value.code == ""

    def test_none_parameters_are_not_sent(self):
        evals, session = client(Reply([]))
        evals.runs.list()
        assert session.sent[0][2] is None
        evals.runs.list(deployment_id=7)
        assert session.sent[1][2] == {"deploymentId": 7}

    def test_an_empty_body_is_none(self):
        evals, _ = client(Reply(None))
        assert evals.tasks.delete("t") is None


class TestSuites:
    def test_create_sends_the_checks_with_it_and_sandboxes_attacks(self):
        evals, session = client(
            Reply({"suiteId": "s", "version": 1, "name": "Attacks", "status": "DRAFT"})
        )
        suite = evals.suites.create(
            "Attacks",
            checks=[check("llm_judge", "q", provider="anthropic")],
            tags=["safety"],
            blocks_are_success=True,
            gate_metric="pass_all_k",
        )
        method, url, _, body = session.sent[0]
        assert (method, url) == ("POST", "/agent-evals/suites")
        assert body["executionMode"] == "sandboxed" and body["blocksAreSuccess"] is True
        assert body["tags"] == '["safety"]' and body["gateThreshold"] == 1.0
        assert json.loads(body["evaluators"][0]["config"]) == {"provider": "anthropic"}
        assert (
            isinstance(suite, Suite) and suite.suite_id == "s" and not suite.published
        )

    def test_update_sends_only_what_was_given_in_the_apis_names(self):
        evals, session = client(Reply({"suiteId": "s", "version": 2}))
        evals.suites.update(
            "s", version=2, name="New", tags=["a", "b"], blocks_are_success=False
        )
        method, url, params, body = session.sent[0]
        assert (method, url, params) == ("PUT", "/agent-evals/suites/s", {"version": 2})
        assert body == {"name": "New", "tags": '["a", "b"]', "blocksAreSuccess": False}

    def test_a_suite_acts_on_itself_through_the_client_it_came_from(self):
        evals, session = client(
            Reply({"suiteId": "s", "version": 1, "status": "DRAFT"}),
            Reply({"taskId": "t"}),
            Reply({"taskId": "t", "suiteId": "s"}),
            Reply({"suiteId": "s", "version": 1, "status": "PUBLISHED"}),
            Reply([{"taskId": "t"}]),
        )
        suite = evals.suites.get("s")
        task = suite.add_task(["hi", "and again"], expectations={"q": "42"})
        assert isinstance(task, Task)
        assert json.loads(session.sent[1][3]["inputMessages"]) == [
            {"role": "user", "content": "hi"},
            {"role": "user", "content": "and again"},
        ]
        assert session.sent[1][3]["taskType"] == "multi_turn"
        assert session.sent[2][2] == {"suiteId": "s", "version": 1}
        assert suite.publish().published
        assert [t.task_id for t in suite.tasks()] == ["t"]

    def test_a_model_not_from_a_client_says_so(self):
        with pytest.raises(AgentServingError, match="not fetched through a client"):
            Suite.from_api({"suiteId": "s"}).publish()

    def test_new_version_carries_settings_and_checks_forward(self):
        evals, session = client(Reply({"suiteId": "s", "version": 2}))
        current = Suite.from_api(
            {
                "suiteId": "s",
                "version": 1,
                "name": "n",
                "tags": "[]",
                "executionMode": "sandboxed",
                "blocksAreSuccess": True,
                "evaluators": [{"type": "no_tool_error"}],
            }
        )
        evals.suites.new_version(current)
        body = session.sent[0][3]
        assert body["suiteId"] == "s" and body["executionMode"] == "sandboxed"
        assert body["evaluators"] == [{"type": "no_tool_error"}]

    def test_copying_a_versions_tasks_names_both_versions(self):
        """A new version's tasks come from the server's copy, not from a re-import.

        The endpoint is the whole point: an import re-authors what it writes, so a
        task promoted from a conversation would lose its source trace and its
        reviewer and land unredacted in the draft.
        """
        evals, session = client(Reply({"imported": 3, "skipped": 1}))

        result = evals.suites.copy_tasks("s", from_version=1, version=2)

        assert result == {"imported": 3, "skipped": 1}
        assert session.sent[0][:3] == (
            "POST",
            "/agent-evals/suites/s/tasks/copy",
            {"version": 2, "fromVersion": 1},
        )

    def test_import_accepts_questions_and_turns_them_into_messages(self):
        evals, session = client(Reply({"imported": 2, "skipped": 0}))
        result = evals.suites.import_tasks(
            "s", [{"question": "one"}, {"inputMessages": "[]"}], version=1
        )
        assert result == {"imported": 2, "skipped": 0}
        rows = session.sent[0][3]
        assert json.loads(rows[0]["inputMessages"]) == [
            {"role": "user", "content": "one"}
        ]
        assert rows[1] == {"inputMessages": "[]"}

    def test_delete_and_find(self):
        evals, session = client(
            Reply(None),
            Reply(
                [
                    {"suiteId": "a", "name": "x", "version": 1},
                    {"suiteId": "a", "name": "x", "version": 3},
                ]
            ),
        )
        evals.suites.delete("a", version=1, force=True)
        assert session.sent[0] == (
            "DELETE",
            "/agent-evals/suites/a",
            {"version": 1, "force": "true"},
            None,
        )
        assert evals.suites.find("x").version == 3


class TestTasks:
    def test_the_promotion_queue_is_a_filter(self):
        evals, session = client(Reply([]))
        evals.tasks.list(pending_redaction=True)
        assert session.sent[0][2] == {"pendingRedaction": "true"}

    def test_forgetting_a_trace_says_what_it_erased(self):
        """A deletion request has to be answerable with what was removed.

        And it is more than the tasks: the verdicts about the trace quote the
        turn, and the analysis model's proposals quote those.
        """
        evals, session = client(
            Reply({"removed": 1, "tasks": 1, "feedback": 2, "triage": 3, "clusters": 1})
        )

        removed = evals.tasks.forget_trace(7, "t")

        assert session.sent[0][:3] == (
            "DELETE",
            "/agent-evals/tasks/from-trace/t",
            {"deploymentId": 7},
        )
        assert removed["tasks"] == 1
        assert removed["feedback"] == 2
        assert removed["triage"] == 3
        assert removed["clusters"] == 1

    def test_promoting_a_trace_and_confirming_its_redaction(self):
        evals, session = client(
            Reply({"taskId": "t", "redactionStatus": "PENDING_REDACTION"}),
            Reply({"taskId": "t", "redactionStatus": "REDACTED"}),
        )
        task = evals.tasks.promote(
            7, "trace-1", expectations={"expected": "44.10"}, category="wrong_answer"
        )
        assert task.needs_review
        assert session.sent[0][1] == "/agent-evals/tasks/from-trace/trace-1"
        assert session.sent[0][2] == {"deploymentId": 7}
        confirmed = task.confirm_redaction(
            input_messages=[{"role": "user", "content": "[redacted]"}]
        )
        assert confirmed.redaction_status == "REDACTED"
        assert (
            json.loads(session.sent[1][3]["inputMessages"])[0]["content"]
            == "[redacted]"
        )

    def test_joining_the_regression_suite_is_one_call(self):
        evals, session = client(Reply({"taskId": "t", "suiteId": "reg"}))
        evals.tasks.add_to_regressions("t")
        assert session.sent[0][:2] == ("POST", "/agent-evals/tasks/t/regressions")

    def test_editing_a_task_sends_only_what_changed(self):
        evals, session = client(
            Reply({"taskId": "t", "category": "wrong_answer"}),
            Reply({"taskId": "t"}),
        )
        task = Task.from_api({"taskId": "t"})
        evals.tasks.update(task, category="wrong_answer")
        assert session.sent[0][:2] == ("PUT", "/agent-evals/tasks/t")
        # the question it did not pass is left alone by the server
        assert session.sent[0][3] == {"category": "wrong_answer"}

        evals.tasks.update("t", ["Analyze GOOGL.", "I'm conservative. Strategies?"])
        body = session.sent[1][3]
        assert body["taskType"] == "multi_turn"
        assert [m["content"] for m in json.loads(body["inputMessages"])] == [
            "Analyze GOOGL.",
            "I'm conservative. Strategies?",
        ]

    def test_one_turn_is_a_single_turn_task(self):
        evals, session = client(Reply({"taskId": "t"}))
        evals.tasks.update("t", "Just tell me what to buy.")
        assert session.sent[0][3]["taskType"] == "single_turn"

    def test_attaching_a_file_sends_its_bytes(self, tmp_path):
        picture = tmp_path / "chart.png"
        picture.write_bytes(b"\x89PNG\r\n")
        evals, session = client(Reply({"name": "chart.png", "size": 7}))

        evals.tasks.attach("t", picture, mime_type="image/png")

        method, path, params, body = session.sent[0]
        assert (method, path) == ("POST", "/agent-evals/tasks/t/attachments/chart.png")
        assert params == {"mimeType": "image/png"}
        # the bytes themselves, not base64 in a JSON field
        assert body == b"\x89PNG\r\n"
        assert session.headers["Content-Type"] == "application/octet-stream"

    def test_attaching_bytes_needs_a_name(self):
        evals, _ = client()
        with pytest.raises(ValueError, match="name="):
            evals.tasks.attach("t", b"\x00\x01")

    def test_reading_an_attachment_returns_its_bytes(self):
        evals, session = client(Reply(content=b"\x89PNG\r\n"))
        assert evals.tasks.attachment("t", "chart.png") == b"\x89PNG\r\n"
        assert session.sent[0][:2] == (
            "GET",
            "/agent-evals/tasks/t/attachments/chart.png",
        )

    def test_removing_an_attachment(self):
        evals, session = client(Reply())
        evals.tasks.remove_attachment("t", "chart.png")
        assert session.sent[0][:2] == (
            "DELETE",
            "/agent-evals/tasks/t/attachments/chart.png",
        )

    def test_a_task_reads_its_own_question(self):
        task = Task.from_api(
            {
                "taskId": "t",
                "inputMessages": json.dumps([{"role": "user", "content": "Hi"}]),
            }
        )
        assert task.question == "Hi"


class TestEvaluators:
    def test_save_and_list(self):
        evals, session = client(
            Reply({"templateId": "e", "name": "quality", "spec": "[]"}),
            Reply([{"templateId": "e", "name": "quality", "spec": '[{"type":"x"}]'}]),
        )
        saved = evals.evaluators.save("quality", [check("llm_judge")])
        assert saved.template_id == "e"
        assert evals.evaluators.find("quality").checks == [{"type": "x"}]

    def test_tags_travel_as_a_json_array_and_read_back_as_a_list(self):
        # The same shape a suite's tags take, so the two are filtered alike.
        evals, session = client(
            Reply(
                {
                    "templateId": "e",
                    "name": "quality",
                    "spec": "[]",
                    "tags": '["musiccustomersupport", "safety"]',
                }
            ),
            Reply(
                {
                    "templateId": "e",
                    "name": "quality",
                    "spec": "[]",
                    "tags": '["safety"]',
                }
            ),
        )
        saved = evals.evaluators.save(
            "quality", [check("llm_judge")], tags=["musiccustomersupport", "safety"]
        )
        assert json.loads(session.sent[0][3]["tags"]) == [
            "musiccustomersupport",
            "safety",
        ]
        assert saved.tag_list == ["musiccustomersupport", "safety"]

        # An edit keeps the tags it was not asked to change, and replaces them when asked.
        evals.evaluators.update(saved, tags=["safety"])
        assert json.loads(session.sent[1][3]["tags"]) == ["safety"]

    def test_a_checks_settings_reach_a_saved_evaluator(self):
        # check() writes a check the way a suite stores it, with the settings
        # in a `config` string; a saved evaluator's spec carries them beside
        # the type, and the server reads them there. Sent as the suite's shape
        # a regex was saved with no pattern the server could see, and every
        # monitor built on it was refused.
        evals, session = client(
            Reply({"templateId": "e", "name": "kind", "spec": "[]"})
        )

        evals.evaluators.save(
            "kind", [check("regex", "answers in kind", pattern="^ok")]
        )

        assert json.loads(session.sent[0][3]["spec"]) == [
            {"type": "regex", "name": "answers in kind", "pattern": "^ok"}
        ]

    def test_a_spec_already_written_that_way_is_left_alone(self):
        evals, session = client(
            Reply({"templateId": "e", "name": "kind", "spec": "[]"})
        )

        evals.evaluators.save("kind", [{"type": "regex", "pattern": "^ok"}])

        assert json.loads(session.sent[0][3]["spec"]) == [
            {"type": "regex", "pattern": "^ok"}
        ]

    def test_update_edits_the_entry_instead_of_adding_another(self):
        """`save` always creates, so an edit used to mean a duplicate name.

        Which is the key sprawl a shared library exists to prevent: two
        evaluators called "quality", and no way to tell which a suite copied.
        """
        evals, session = client(
            Reply(
                {
                    "templateId": "e",
                    "name": "quality",
                    "description": "what good looks like",
                    "spec": '[{"type":"llm_judge"}]',
                }
            ),
        )
        current = EvaluatorTemplate.from_api(
            {
                "templateId": "e",
                "name": "quality",
                "description": "what good looks like",
                "spec": '[{"type":"llm_judge"}]',
            }
        )

        evals.evaluators.update(current, name="answer quality")

        body = session.sent[0][3]
        # the id is what makes it an edit; everything unnamed is carried
        assert body["templateId"] == "e"
        assert body["name"] == "answer quality"
        assert body["description"] == "what good looks like"
        assert body["spec"] == '[{"type":"llm_judge"}]'

    def test_installing_defaults_leaves_existing_ones_alone(self):
        from hopsworks_agents.eval.judge_config import _default_templates

        first = _default_templates()[0]["name"]
        evals, session = client(
            Reply([{"templateId": "e", "name": first, "spec": "[]"}])
        )
        written = evals.evaluators.install_defaults()
        assert first not in written and len(written) == len(_default_templates()) - 1

    def test_judge_models_asks_the_provider(self):
        evals, session = client(Reply(["gpt-5.6-terra"]))
        assert evals.evaluators.judge_models("openai") == ["gpt-5.6-terra"]
        assert session.sent[0][2] == {"provider": "openai"}


class TestRuns:
    def test_start_uses_query_parameters_and_wait_polls_to_the_end(self):
        evals, session = client(
            Reply({"runId": "r", "status": "PENDING"}),
            Reply({"runId": "r", "status": "RUNNING"}),
            Reply({"runId": "r", "status": "SUCCEEDED"}),
        )
        run = evals.runs.start("s", 7, version=2, n_trials=3)
        assert session.sent[0][2] == {
            "suiteId": "s",
            "version": 2,
            "deploymentId": 7,
            "nTrials": 3,
            "start": "true",
        }
        assert session.sent[0][3] is None
        done = run.wait(poll_s=0)
        assert done.succeeded and len(session.sent) == 3

    def test_wait_gives_up_with_the_status_it_saw(self):
        evals, _ = client(Reply({"runId": "r", "status": "RUNNING"}))
        with pytest.raises(AgentServingError, match="still RUNNING"):
            evals.runs.wait("r", timeout_s=0, poll_s=0)

    def test_wait_says_when_it_is_the_deployment_that_is_not_up(self):
        # A run fired at a stopped deployment sits at RUNNING and looks exactly
        # like one that is working: the runner waits five minutes for the
        # deployment before failing. "still RUNNING" alone sent someone to read
        # a job log to learn the agent was off.
        evals, session = client(
            Reply({"runId": "r", "status": "RUNNING", "deploymentId": 7}),
            Reply({"id": 7, "name": "support", "status": "Stopped"}),
        )
        with pytest.raises(AgentServingError, match="deployment 7 is Stopped"):
            evals.runs.wait("r", timeout_s=0, poll_s=0)
        assert session.sent[1][1] == "/serving/7"

    def test_a_serving_deployment_is_not_reported_as_something_to_wait_for(self):
        evals, _ = client(Reply({"id": 7, "status": "Running"}))
        assert evals.runs.agent_status(Run(run_id="r", deployment_id=7)) == ""

    def test_an_idle_deployment_counts_as_up_because_a_request_wakes_it(self):
        evals, _ = client(Reply({"id": 7, "status": "Idle"}))
        assert evals.runs.agent_status(Run(run_id="r", deployment_id=7)) == ""

    def test_an_opinion_about_analysis_is_sent_and_silence_is_not(self):
        # A failure nobody explains is one somebody reads twice, so a new job
        # gets the analysis -- but that default belongs to the server, not to
        # this call. Sending True for a caller who said nothing turned analysis
        # back on for a job where somebody had deliberately turned it off,
        # because ensure now applies what it is sent.
        evals, session = client(Reply({"name": "nightly"}), Reply({"name": "n2"}))
        evals.jobs.ensure_eval_job(7)
        assert "analyseFailures" not in session.sent[0][3]

        evals.jobs.ensure_eval_job(7, analyse_failures=False)
        assert session.sent[1][3]["analyseFailures"] is False

    def test_the_evaluation_job_configures_the_analysis_too(self):
        # One workflow -- evaluate, then explain what broke -- so the analysis
        # job is configured from here rather than answering the environment,
        # the resources and a schedule twice.
        evals, session = client(Reply({"name": "nightly"}))
        evals.jobs.ensure_eval_job(
            7,
            analyse_failures=True,
            judge_provider="anthropic",
            judge_model="claude-haiku-4-5",
            analysis_budget_calls=100,
        )
        sent = session.sent[0][3]
        assert sent["analyseFailures"] is True
        # one model setting, used by the checks that name none and by the analysis
        assert sent["judgeModel"] == "claude-haiku-4-5"
        assert sent["analysisBudgetCalls"] == 100

    def test_a_manual_job_run_can_name_the_window_it_samples(self):
        # A scheduled fire grades what arrived since the last one. Trying an
        # evaluator out means naming a window -- yesterday afternoon, twenty
        # conversations -- without changing what every fire does.
        evals, session = client(Reply([{"runId": "r"}]))
        since = datetime(2026, 10, 15, 14, tzinfo=timezone.utc)
        evals.jobs.run_eval_job("nightly", since=since, sample=20, strategy="oldest")
        sent = session.sent[0][2]
        assert sent["from"] == int(since.timestamp() * 1000)
        assert sent["sample"] == 20 and sent["strategy"] == "oldest"

    def test_comparing_two_runs_says_what_changed(self):
        # The comparison a gate asks for. A task passes only when every gradable
        # trial of it passed -- one attempt working is not the agent being
        # reliable at it -- so t2 regressed even though it passed once.
        evals, session = client(
            Reply({"runId": "old", "suiteId": "s", "suiteVersion": 1}),
            Reply({"runId": "new", "suiteId": "s", "suiteVersion": 1}),
            Reply(
                [
                    {"taskId": "t1", "status": "PASSED"},
                    {"taskId": "t2", "status": "PASSED"},
                    {"taskId": "t2", "status": "PASSED"},
                    {"taskId": "t3", "status": "FAILED"},
                    {"taskId": "gone", "status": "PASSED"},
                ]
            ),
            Reply(
                [
                    {"taskId": "t1", "status": "PASSED"},
                    {"taskId": "t2", "status": "PASSED"},
                    {"taskId": "t2", "status": "FAILED"},
                    {"taskId": "t3", "status": "PASSED"},
                    {"taskId": "fresh", "status": "FAILED"},
                ]
            ),
            Reply(
                [
                    {
                        "taskId": "t1",
                        "inputMessages": json.dumps(
                            [{"role": "user", "content": "Analyze TSLA."}]
                        ),
                    }
                ]
            ),
            Reply([]),
        )

        diff = evals.runs.compare("old", "new")

        assert [t.task_id for t in diff.regressed] == ["t2"]
        assert [t.task_id for t in diff.fixed] == ["t3"]
        assert [t.task_id for t in diff.added] == ["fresh"]
        assert [t.task_id for t in diff.removed] == ["gone"]
        assert [t.task_id for t in diff.unchanged] == ["t1"]
        assert diff.same_suite and not diff.partial
        # a difference names the question it asks, when the tasks are readable
        assert diff.unchanged[0].question == "Analyze TSLA."
        assert "1 regressed" in str(diff) and "1 added" in str(diff)

    def test_the_failure_history_says_which_failures_are_new(self):
        # "seventeen failed" is not something anyone can act on; "one of them
        # is new, one is a regression, and that one has been failing for seven
        # runs" is.
        evals, session = client(
            Reply(
                [
                    {
                        "suiteId": "s1",
                        "suiteName": "Introduces itself",
                        "suiteVersion": 1,
                        "latestRunId": "r3",
                        "previousRunId": "r2",
                        "runsCompared": 10,
                        "newFailures": 1,
                        "regressions": 1,
                        "fixed": 2,
                        "persistent": 1,
                        "passing": 9,
                        "tasks": [
                            {
                                "taskId": "fresh",
                                "status": "new",
                                "age": 1,
                                "previousVerdict": "absent",
                                "currentVerdict": "failed",
                            },
                            {
                                "taskId": "broke",
                                "status": "regression",
                                "age": 1,
                                "previousVerdict": "passed",
                                "currentVerdict": "failed",
                            },
                            {
                                "taskId": "stuck",
                                "status": "persistent",
                                "age": 7,
                                "previousVerdict": "failed",
                                "currentVerdict": "failed",
                            },
                            {
                                "taskId": "mended",
                                "status": "fixed",
                                "age": 0,
                                "previousVerdict": "failed",
                                "currentVerdict": "passed",
                            },
                        ],
                    }
                ]
            )
        )

        [suite] = evals.runs.failure_history(7, depth=10)

        assert session.sent[0][1] == "/agent-evals/failure-history"
        assert session.sent[0][2] == {"deploymentId": 7, "depth": 10}
        assert [t.task_id for t in suite.new] == ["fresh"]
        assert [t.task_id for t in suite.regressed] == ["broke"]
        assert [t.task_id for t in suite.repaired] == ["mended"]
        assert suite.still_failing[0].age == 7
        assert "1 new, 1 regressed" in str(suite)

    def test_a_run_says_what_it_changed(self):
        # The question belongs to the run: asked of a run three days old,
        # "which of these is new" means new then.
        evals, session = client(Reply([{"suiteId": "s1", "newFailures": 1}]))
        run = Run(run_id="r3", deployment_id=7, suite_id="s1")
        evals._bind(run)

        [suite] = run.changes()

        assert session.sent[0][2] == {"deploymentId": 7, "runId": "r3"}
        assert suite.new_failures == 1

    def test_a_run_compares_itself_to_a_later_one(self):
        evals, _ = client(
            Reply([{"taskId": "t1", "status": "PASSED"}]),
            Reply([{"taskId": "t1", "status": "FAILED"}]),
        )
        previous = Run(run_id="old", suite_id="s", suite_version=1)
        latest = Run(run_id="new", suite_id="s", suite_version=1)
        evals._bind(previous)

        diff = previous.compare_to(latest, questions=False)

        assert [t.task_id for t in diff.regressed] == ["t1"]

    def test_comparing_runs_of_different_suites_says_so(self):
        # Two suites share no tasks, so everything reads as added or removed:
        # the caller should not have to notice that from the numbers.
        evals, _ = client(
            Reply(
                {"runId": "a", "suiteId": "s1", "suiteVersion": 1, "status": "FAILED"}
            ),
            Reply({"runId": "b", "suiteId": "s2", "suiteVersion": 1}),
            Reply([{"taskId": "t1", "status": "PASSED"}]),
            Reply([{"taskId": "t2", "status": "FAILED"}]),
        )

        diff = evals.runs.compare("a", "b", questions=False)

        assert not diff.same_suite
        # one of them failed, so a missing task is not necessarily a removal
        assert diff.partial
        assert not diff.regressed and not diff.unchanged
        assert len(diff.added) == 1 and len(diff.removed) == 1
        assert "different suites" in str(diff)

    def test_a_harness_failure_is_not_a_regression(self):
        # INFRA_ERROR is the harness, not the agent; a task whose every trial
        # was one is ungradable, and losing the verdict is not a failure.
        evals, _ = client(
            Reply({"runId": "a", "suiteId": "s", "suiteVersion": 1}),
            Reply({"runId": "b", "suiteId": "s", "suiteVersion": 1}),
            Reply([{"taskId": "t1", "status": "PASSED"}]),
            Reply([{"taskId": "t1", "status": "INFRA_ERROR"}]),
        )

        diff = evals.runs.compare("a", "b", questions=False)

        assert not diff.regressed
        assert [t.task_id for t in diff.ungraded] == ["t1"]
        assert diff.ungraded[0].after == "ungradable"

    def test_sampling_production_needs_something_to_grade_with(self):
        evals, session = client(Reply({"runId": "r", "runType": "ONLINE_SAMPLE"}))
        with pytest.raises(AgentServingError, match="evaluator or a suite"):
            evals.runs.sample(7)
        since = datetime(2026, 9, 1, tzinfo=timezone.utc)
        evals.runs.sample(7, evaluator="tmpl", since=since, sample=10)
        params = session.sent[0][2]
        assert params["templateId"] == "tmpl" and params["from"] == int(
            since.timestamp() * 1000
        )
        assert "to" not in params

    def test_trials_results_metrics_and_a_review(self):
        evals, session = client(
            Reply(
                [{"runId": "r", "trialId": "r/1", "taskId": "t", "status": "PASSED"}]
            ),
            Reply(
                [
                    {
                        "trialId": "r/1",
                        "evaluatorName": "q",
                        "passed": True,
                        "assertionsJson": '{"a":1}',
                    }
                ]
            ),
            Reply([{"runId": "r", "metricName": "pass_rate", "metricValue": 1.0}]),
            Reply(None),
        )
        [trial] = evals.runs.trials("r")
        assert trial.passed
        [result] = evals.runs.results("r", trial)
        assert result.assertions == {"a": 1} and session.sent[1][2] == {
            "trialId": "r/1"
        }
        assert evals.runs.metrics("r")[0].metric_value == 1.0
        evals.runs.review_trial("r", trial, passed=False, reason="wrong")
        assert session.sent[3][3] == {
            "taskId": "t",
            "passed": False,
            "score": None,
            "reason": "wrong",
        }

    def test_a_review_may_be_about_one_judge(self):
        evals, session = client(Reply(None))
        evals.runs.review_trial("r", "r/1", passed=True, evaluator="hallucination")
        assert session.sent[0][3]["evaluatorName"] == "hallucination"


class TestJobs:
    def test_the_eval_job_is_configured_with_suite_and_evaluator_references(self):
        evals, session = client(Reply({"name": "agent_eval", "created": True}))
        suite = Suite.from_api({"suiteId": "s", "version": 2})
        job = evals.jobs.ensure_eval_job(
            7, suites=[suite, "x:1"], evaluators=["tmpl"], monitor=True, cores=2
        )
        assert job.created
        assert session.sent[0][2] == {"deploymentId": 7}
        assert session.sent[0][3] == {
            "suites": ["s:2", "x:1"],
            "evaluators": ["tmpl"],
            "monitor": True,
            # the analysis comes with it unless the caller says otherwise
            "cores": 2,
        }

    def test_the_review_job_settings_are_named_as_the_model_names_them(self):
        evals, session = client(
            Reply({"name": "agent_feedback_review", "sources": "feedback,errors"})
        )
        job = evals.jobs.ensure_review_job(
            7,
            provider="custom",
            base_url="https://gw/v1",
            headers={"X-Tenant": "acme"},
            sources=["feedback", "errors"],
            read_source_code=False,
            budget_calls=50,
        )
        body = session.sent[0][3]
        assert body == {
            "provider": "custom",
            "baseUrl": "https://gw/v1",
            "headers": '{"X-Tenant": "acme"}',
            "sources": "feedback,errors",
            "readSourceCode": False,
            "budgetCalls": 50,
        }
        assert job.source_list == ["feedback", "errors"]

    def test_analysing_since_the_last_run_a_window_or_one_trace(self):
        evals, session = client(
            Reply({"runId": "a"}), Reply({"runId": "b"}), Reply({"runId": "c"})
        )
        evals.jobs.analyse("job")
        evals.jobs.analyse("job", since=1000, until=2000)
        evals.jobs.analyse("job", trace_id="305b")
        assert session.sent[0][2] is None
        assert session.sent[1][2] == {"from": 1000, "to": 2000}
        assert session.sent[2][2] == {"traceId": "305b"}
        assert session.sent[0][:2] == ("POST", "/agent-evals/review-jobs/job/run")

    def test_regressions(self):
        evals, session = client(
            Reply({"exists": True, "name": "a_feedback_regressions", "taskCount": 3}),
            Reply({"runId": "r"}),
        )
        assert evals.jobs.regressions(7).task_count == 3
        assert evals.jobs.run_regressions(7).run_id == "r"
        assert session.sent[1] == (
            "POST",
            "/agent-evals/regressions/run",
            {"deploymentId": 7},
            None,
        )


def trace_row(trace_id, start_ms, messages):
    return {
        "traceId": trace_id,
        "spanId": "s",
        "startTimeNs": start_ms * 1_000_000,
        "sessionId": "conv",
        "messages": json.dumps(messages),
    }


class TestTracing:
    def test_traces_are_listed_and_searched(self):
        evals, agent7, session = agent(
            Reply(
                {
                    "items": [trace_row("t1", 10, [{"role": "user", "content": "hi"}])],
                    "count": 1,
                }
            )
        )
        [trace] = agent7.traces(search="hi", search_field="messages", limit=5)
        assert session.sent[0][1] == "/otel/servings/7/traces"
        assert session.sent[0][2] == {
            "limit": 5,
            "search": "hi",
            "searchField": "messages",
        }
        assert trace.trace_id == "t1" and trace.conversation[0]["content"] == "hi"
        assert trace.started_at == datetime.fromtimestamp(0.01, tz=timezone.utc)

    def test_a_session_becomes_the_conversation_the_user_had(self):
        evals, agent7, _ = agent(
            Reply(
                {
                    "items": [
                        trace_row(
                            "later",
                            20,
                            [
                                {"role": "user", "content": "hi"},
                                {"role": "assistant", "content": "hello"},
                                {"role": "user", "content": "my order?"},
                                {"role": "assistant", "content": "shipped"},
                            ],
                        ),
                        trace_row(
                            "first",
                            10,
                            [
                                {"role": "user", "content": "hi"},
                                {"role": "assistant", "content": "hello"},
                            ],
                        ),
                    ]
                }
            )
        )
        turns = agent7.conversation("conv")
        assert [t["trace_id"] for t in turns] == ["first", "later"]
        assert turns[1] == {
            "trace_id": "later",
            "user": "my order?",
            "assistant": "shipped",
        }

    def test_a_whole_trace_reads_its_tool_calls_off_the_attributes(self):
        evals, agent7, _ = agent(
            Reply(
                {
                    "spans": [
                        {"traceId": "t", "spanId": "root", "startTimeNs": 1},
                        {
                            "traceId": "t",
                            "spanId": "tool",
                            "parentSpanId": "root",
                            "name": "lookup",
                            "startTimeNs": 2,
                            "statusCode": "STATUS_CODE_ERROR",
                        },
                    ],
                    "spanAttributes": [
                        {
                            "spanId": "tool",
                            "attrKey": "openinference.span.kind",
                            "attrValue": "TOOL",
                        },
                        {
                            "spanId": "tool",
                            "attrKey": "tool.name",
                            "attrValue": "lookup_customer",
                        },
                        {
                            "spanId": "tool",
                            "attrKey": "input.value",
                            "attrValue": '{"key": "629e"}',
                        },
                    ],
                    "totalInputTokens": 12,
                }
            )
        )
        trace = agent7.trace("t")
        assert trace.trace_id == "t" and trace.root["spanId"] == "root"
        [call] = trace.tool_calls
        assert (
            call["name"] == "lookup_customer" and call["status"] == "STATUS_CODE_ERROR"
        )
        assert call["arguments"] == '{"key": "629e"}'

    def test_feedback_is_given_read_and_taken_back(self):
        evals, agent7, session = agent(
            Reply(
                {
                    "feedbackId": "f",
                    "traceId": "t",
                    "verdict": "negative",
                    "reviewer": "me@x",
                }
            ),
            Reply(
                {
                    "count": 1,
                    "items": [
                        {
                            "feedbackId": "f",
                            "traceId": "t",
                            "verdict": "negative",
                            "reviewer": "detector:tool_error",
                        }
                    ],
                }
            ),
            Reply(None),
        )
        deployment = agent7
        given = deployment.give_feedback(
            "t", "negative", issue_category="wrong_tool", corrected_answer="use the key"
        )
        assert given.source == "human"
        assert session.sent[0][3] == {
            "verdict": "negative",
            "issueCategory": "wrong_tool",
            "correctedAnswer": "use the key",
        }
        page = deployment.feedback(verdict="negative", limit=10)
        assert page.count == 1 and page.feedback[0].source == "detector"
        assert session.sent[1][2] == {"limit": 10, "offset": 0, "verdict": "negative"}
        deployment.retract_feedback("t")
        assert session.sent[2][:2] == ("DELETE", "/otel/servings/7/traces/t/feedback")
        with pytest.raises(ValueError, match="verdict must be"):
            deployment.give_feedback("t", "meh")

    def test_an_end_users_verdict_is_retracted_on_their_behalf(self):
        """An end user has no session here, so a member takes it back for them.

        The route has always accepted it and the SDK had no way to say it, which
        left the one verdict a person cannot retract themselves reachable only
        from the UI.
        """
        evals, agent7, session = agent(Reply(None))

        agent7.retract_feedback("t", reviewer="user:customer-7")

        assert session.sent[0][:3] == (
            "DELETE",
            "/otel/servings/7/traces/t/feedback",
            {"reviewer": "user:customer-7"},
        )

    def test_all_feedback_walks_the_pages(self):
        evals, agent7, session = agent(
            Reply({"count": 3, "items": [{"feedbackId": "1"}, {"feedbackId": "2"}]}),
            Reply({"count": 3, "items": [{"feedbackId": "3"}]}),
        )
        rows = agent7.all_feedback(limit=2, verdict="negative")
        assert [r.feedback_id for r in rows] == ["1", "2", "3"]
        assert session.sent[1][2]["offset"] == 2

    def test_the_summary_is_about_people_unless_asked_otherwise(self):
        evals, agent7, session = agent(
            Reply(
                {
                    "from": 1,
                    "to": 2,
                    "positive": 3,
                    "negative": 1,
                    "issueCategories": {"x": 1},
                }
            )
        )
        summary = agent7.feedback_summary(since=1, until=2)
        assert (
            summary.positive == 3
            and summary.from_ms == 1
            and summary.issue_categories == {"x": 1}
        )
        assert session.sent[0][2] == {"from": 1, "to": 2, "source": "human"}

    def test_triage_and_the_decision_that_calibrates_it(self):
        evals, agent7, session = agent(
            Reply(
                [
                    {
                        "triageId": "f/run",
                        "feedbackId": "f",
                        "suspectedCodeBug": True,
                        "codeFindings": json.dumps(
                            [
                                {
                                    "file": "a.py",
                                    "line": 3,
                                    "finding": "x",
                                    "verified": True,
                                }
                            ]
                        ),
                    }
                ]
            ),
            Reply(None),
            Reply({"decisions": 4, "accepted": 3, "acceptanceRate": 0.75}),
        )
        deployment = agent7
        [triage] = deployment.triage(["f"])
        assert session.sent[0][2] == {"feedbackId": ["f"]}
        assert (
            triage.suspected_code_bug
            and triage.findings[0]["file"] == "a.py"
            and not triage.decided
        )
        deployment.decide_triage(triage, "accepted")
        # The id is `feedback/run`, so it travels as a parameter: in the path it
        # was two segments where the route declares one, and the decision 404'd.
        assert session.sent[1] == (
            "PUT",
            "/otel/servings/7/feedback/triage/decision",
            {"triageId": "f/run", "decision": "accepted"},
            None,
        )
        assert deployment.calibration().acceptance_rate == 0.75
        assert deployment.triage([]) == []

    def test_clusters_are_promoted_from_their_representative(self):
        evals, agent7, session = agent(
            Reply(
                [
                    {
                        "clusterId": "c",
                        "size": 2,
                        "representativeTraceId": "t",
                        "representativeFeedbackId": "f",
                    }
                ]
            ),
            Reply(
                [
                    {
                        "triageId": "x",
                        "feedbackId": "f",
                        "normalizedCorrection": "Use the key.",
                    }
                ]
            ),
            Reply(
                {
                    "count": 1,
                    "items": [
                        {
                            "feedbackId": "f",
                            "traceId": "t",
                            "traceMessages": json.dumps(
                                [
                                    {"role": "user", "content": "where is order 42?"},
                                    {"role": "assistant", "content": "no idea"},
                                ]
                            ),
                        }
                    ],
                }
            ),
            Reply({"taskId": "task"}),
            Reply(None),
        )
        deployment = agent7
        [cluster] = deployment.clusters()
        assert session.sent[0][2] == {"status": "open"}
        task = deployment.promote_cluster(cluster)
        assert task.task_id == "task"
        assert session.sent[3][1] == "/agent-evals/tasks/from-trace/t"
        assert session.sent[3][3]["expectations"] == {"expected": "Use the key."}
        # The question, off the verdict the cluster named. Without it the
        # promotion writes a task with no content, which the redaction gate then
        # refuses to confirm -- a cluster promoted into something unusable.
        assert json.loads(session.sent[3][3]["inputMessages"]) == [
            {"role": "user", "content": "where is order 42?"}
        ]
        assert session.sent[4] == (
            "PUT",
            "/otel/servings/7/feedback/clusters/c",
            {"status": "promoted", "promotedTaskId": "task"},
            None,
        )

    def test_promoting_a_verdict_carries_its_conversation(self):
        # A failure on the fourth turn cannot be reproduced by asking the fourth
        # question of a fresh agent, and a conversation is several traces
        # sharing a session id -- so the turns that set it up are in the traces
        # before the one the verdict is about.
        evals, agent7, session = agent(
            Reply(
                {
                    "items": [
                        {
                            "traceId": "t1",
                            "startTimeNs": 1,
                            "messages": json.dumps(
                                [{"role": "user", "content": "my email is d@e.com"}]
                            ),
                        },
                        {
                            "traceId": "t2",
                            "startTimeNs": 2,
                            "messages": json.dumps(
                                [{"role": "user", "content": "ticker MSFT"}]
                            ),
                        },
                        {
                            "traceId": "t3",
                            "startTimeNs": 3,
                            "messages": json.dumps(
                                [{"role": "user", "content": "propose strategies"}]
                            ),
                        },
                        {
                            "traceId": "t4",
                            "startTimeNs": 4,
                            "messages": json.dumps(
                                [{"role": "user", "content": "a later, unrelated turn"}]
                            ),
                        },
                    ]
                }
            ),
            Reply({"taskId": "task"}),
        )
        row = Feedback(
            feedback_id="f",
            trace_id="t3",
            session_id="s1",
            issue_category="wrong_answer",
            corrected_answer="Strategies, with the disclaimer.",
        )

        task = agent7.promote_feedback(row)

        assert task.task_id == "task"
        # The session id travels as a query parameter, not in the path.
        assert session.sent[0][1] == "/otel/servings/7/traces/sessions"
        assert session.sent[0][2]["sessionId"] == "s1"
        # Up to and including the verdict's turn, and no further.
        assert json.loads(session.sent[1][3]["inputMessages"]) == [
            {"role": "user", "content": "my email is d@e.com"},
            {"role": "user", "content": "ticker MSFT"},
            {"role": "user", "content": "propose strategies"},
        ]
        # The category and the correction are on the verdict, not the trace, so
        # promoting one without them produced a task the redaction gate refuses.
        assert session.sent[1][3]["category"] == "wrong_answer"
        assert session.sent[1][3]["expectations"] == {
            "expected": "Strategies, with the disclaimer."
        }

    def test_a_repeated_request_is_kept(self):
        # Deduplicating by text lost a genuine repeat: a user who says
        # "continue" twice got one "continue", so the task replayed a different
        # conversation from the one that failed -- and if the failure needs the
        # second continuation, it cannot be reproduced at all.
        evals, agent7, session = agent(
            Reply(
                {
                    "items": [
                        {
                            "traceId": f"t{i}",
                            "startTimeNs": i,
                            "messages": json.dumps([{"role": "user", "content": text}]),
                        }
                        for i, text in enumerate(
                            ["continue", "continue", "what next?"], start=1
                        )
                    ]
                }
            ),
            Reply({"taskId": "task"}),
        )

        agent7.promote_feedback(
            Feedback(feedback_id="f", trace_id="t3", session_id="s")
        )

        assert json.loads(session.sent[1][3]["inputMessages"]) == [
            {"role": "user", "content": "continue"},
            {"role": "user", "content": "continue"},
            {"role": "user", "content": "what next?"},
        ]

    def test_a_trace_that_repeats_the_history_is_not_counted_twice(self):
        # The other half: some frameworks put the whole conversation on every
        # trace, so appending each trace's turns blindly would say everything
        # twice. Position decides, not text.
        evals, agent7, session = agent(
            Reply(
                {
                    "items": [
                        {
                            "traceId": "t1",
                            "startTimeNs": 1,
                            "messages": json.dumps(
                                [{"role": "user", "content": "first"}]
                            ),
                        },
                        {
                            "traceId": "t2",
                            "startTimeNs": 2,
                            "messages": json.dumps(
                                [
                                    {"role": "user", "content": "first"},
                                    {"role": "user", "content": "second"},
                                ]
                            ),
                        },
                    ]
                }
            ),
            Reply({"taskId": "task"}),
        )

        agent7.promote_feedback(
            Feedback(feedback_id="f", trace_id="t2", session_id="s")
        )

        assert json.loads(session.sent[1][3]["inputMessages"]) == [
            {"role": "user", "content": "first"},
            {"role": "user", "content": "second"},
        ]

    def test_a_cluster_promotes_the_member_whose_correction_it_uses(self):
        # A cluster groups by failure signature, not by question, so its members
        # ask different things. Pairing one member's correction with another's
        # question wrote an expected answer to a question nobody asked.
        evals, agent7, session = agent(
            Reply(
                [
                    {
                        "clusterId": "c",
                        "size": 2,
                        "representativeTraceId": "t-rep",
                        "representativeFeedbackId": "f-rep",
                    }
                ]
            ),
            Reply(
                [
                    {"triageId": "x", "feedbackId": "f-rep", "traceId": "t-rep"},
                    {
                        "triageId": "y",
                        "feedbackId": "f-other",
                        "traceId": "t-other",
                        "normalizedCorrection": "Order 43 was delivered on Monday.",
                    },
                ]
            ),
            Reply(
                {"count": 1, "items": [{"feedbackId": "f-other", "traceId": "t-other"}]}
            ),
            Reply({"taskId": "task"}),
            Reply(None),
        )
        [cluster] = agent7.clusters()

        agent7.promote_cluster(cluster)

        # The correcting member's trace, so the question and the expected answer
        # are the same exchange.
        assert session.sent[3][1] == "/agent-evals/tasks/from-trace/t-other"
        assert session.sent[3][3]["expectations"] == {
            "expected": "Order 43 was delivered on Monday."
        }

    def test_promoting_one_turn_on_its_own_is_still_possible(self):
        evals, agent7, session = agent(Reply({"taskId": "task"}))
        row = Feedback(
            feedback_id="f",
            trace_id="t3",
            session_id="s1",
            trace_messages=json.dumps(
                [{"role": "user", "content": "propose strategies"}]
            ),
        )

        agent7.promote_feedback(row, conversation=False)

        # No session read at all: the one turn, as the verdict's trace has it.
        assert session.sent[0][1] == "/agent-evals/tasks/from-trace/t3"
        assert json.loads(session.sent[0][3]["inputMessages"]) == [
            {"role": "user", "content": "propose strategies"}
        ]

    def test_a_cluster_takes_a_correction_from_any_member(self):
        # A cluster is one failure seen several times and only some of those
        # verdicts carry a correction. Insisting on the representative's left
        # the task with nothing to grade against, and the judge it is graded by
        # reads the expected answer.
        evals, agent7, session = agent(
            Reply(
                [
                    {
                        "clusterId": "c",
                        "size": 2,
                        "representativeTraceId": "t",
                        "representativeFeedbackId": "f",
                    }
                ]
            ),
            Reply(
                [
                    {"triageId": "x", "feedbackId": "f"},
                    {
                        "triageId": "y",
                        "feedbackId": "other",
                        "normalizedCorrection": "Say it with the disclaimer.",
                    },
                ]
            ),
            Reply({"count": 1, "items": [{"feedbackId": "f", "traceId": "t"}]}),
            Reply({"taskId": "task"}),
            Reply(None),
        )
        [cluster] = agent7.clusters()

        agent7.promote_cluster(cluster)

        assert session.sent[3][3]["expectations"] == {
            "expected": "Say it with the disclaimer."
        }

    def test_dismiss_rename_and_reopen(self):
        evals, agent7, session = agent(Reply(None), Reply(None), Reply(None))
        deployment = agent7
        deployment.dismiss_cluster("c", "working_as_intended")
        deployment.rename_cluster(
            Cluster.from_api({"clusterId": "c"}), "Ignores the key"
        )
        deployment.reopen_cluster("c")
        assert session.sent[0][2] == {
            "status": "dismissed",
            "dismissReason": "working_as_intended",
        }
        assert session.sent[1][2] == {"label": "Ignores the key"}
        assert session.sent[2][2] == {"status": "open"}

    def test_metrics_gates_and_canary(self):
        evals, agent7, session = agent(
            Reply(
                [
                    {
                        "windowStart": 1,
                        "windowEnd": 2,
                        "traceCount": 5,
                        "traceErrorCount": 1,
                    }
                ]
            ),
            Reply([{"toolName": "lookup", "toolCallCount": 3}]),
            Reply(
                {
                    "passed": False,
                    "hasEvidence": True,
                    "checks": [{"suiteName": "s", "passed": False}],
                }
            ),
            Reply([{"runId": "r"}]),
        )
        deployment = agent7
        assert deployment.trace_metrics(since=1, until=2)[0].trace_error_count == 1
        assert session.sent[0][2] == {"from": 1, "to": 2}
        assert deployment.tool_metrics()[0].tool_name == "lookup"
        gates = deployment.gates()
        assert not gates.passed and gates.gate_checks[0].suite_name == "s"
        assert deployment.canary()[0].run_id == "r"
        assert session.sent[3] == (
            "POST",
            "/agent-evals/canary",
            {"deploymentId": 7},
            None,
        )

    def test_analyse_creates_the_job_when_the_deployment_has_none(self):
        evals, agent7, session = agent(
            Reply([]), Reply({"name": "agent_feedback_review"}), Reply({"runId": "r"})
        )
        run = agent7.analyse()
        assert run.run_id == "r"
        assert session.sent[1][:2] == ("POST", "/agent-evals/review-jobs")
        assert (
            session.sent[2][1] == "/agent-evals/review-jobs/agent_feedback_review/run"
        )


class FakeHopsworksClient:
    """What hopsworks.login() leaves behind: the connected client, with the project on it."""

    def __init__(self, replies):
        self._base_url = "https://h:443/hopsworks-api/api"
        self._project_id = 1
        self._project_name = "p"
        self.replies = list(replies)
        self.sent = []
        self.streamed = []

    def _send_request(
        self,
        method,
        path_params,
        query_params=None,
        headers=None,
        data=None,
        stream=False,
        timeout=None,
    ):
        self.sent.append(
            (method, "/" + "/".join(str(p) for p in path_params), query_params, data)
        )
        self.streamed.append(bool(stream))
        return self.replies.pop(0) if self.replies else {}


class TestConnectedClient:
    def test_get_agent_serving_rides_the_connected_client(self, monkeypatch):
        from hopsworks_common import client as hopsworks_client
        from hopsworks_common.project import Project

        fake = FakeHopsworksClient([SERVING])
        monkeypatch.setattr(hopsworks_client, "_client", fake)
        project = Project.__new__(Project)
        project._id = 1
        agents = project.get_agent_serving()
        assert isinstance(agents, AgentServing)
        agent7 = agents.get_agent("support")
        assert agent7.name == "support"
        assert fake.sent[0][:3] == ("GET", "/project/1/serving", {"name": "support"})

    def test_an_attachment_rides_the_connected_client_as_bytes(self, monkeypatch):
        """The wrapper has to take `data`, which is what Transport always passes.

        It was once missing, so every call through a connected client -- not only
        the ones carrying bytes -- died on an unexpected keyword argument.
        """
        from hopsworks_agents.eval.sdk._transport import (
            HopsworksClientSession,
            Transport,
        )

        fake = FakeHopsworksClient([b"\x89PNG...", {"ok": True}])
        http = Transport("https://h", 1, session=HopsworksClientSession(fake))

        got = http.request(
            "POST",
            "/agent-evals/tasks/7/attachments/chart.png",
            data=b"\x89PNG...",
            headers={"content-type": "image/png"},
            raw=True,
        )
        assert got == b"\x89PNG..."
        assert fake.sent[0][3] == b"\x89PNG..."
        # Streamed, which is the only way bytes come back undecoded: the client
        # parses every other reply as JSON, so reading an attachment died on the
        # picture's first byte with "Expecting value: line 1 column 1".
        assert fake.streamed[0] is True

        # and the ordinary JSON path still goes through as a JSON body, unstreamed
        assert http.request("GET", "/agent-evals/evaluators") == {"ok": True}
        assert fake.streamed[1] is False

    def test_the_connected_transport_is_none_when_nothing_is_connected(
        self, monkeypatch
    ):
        from hopsworks_agents.eval.sdk._transport import connected_transport
        from hopsworks_common import client as hopsworks_client

        monkeypatch.setattr(hopsworks_client, "_client", None)
        assert connected_transport() is None


class TestAgents:
    def test_an_agent_is_fetched_by_name_or_id_and_refused_for_a_model(self):
        evals, session = client(Reply(SERVING), Reply(SERVING))
        by_name = evals.get_agent("support")
        assert session.sent[0][:3] == ("GET", "/serving", {"name": "support"})
        by_id = evals.get_agent("7")
        assert session.sent[1][:2] == ("GET", "/serving/7")
        assert by_name.id == by_id.id == 7 and by_name.name == "support"
        assert repr(by_name) == "Agent(7, name='support')"

        evals, _ = client(Reply({"usrMsg": "Serving not found"}, status=404))
        assert evals.get_agent("missing") is None

        evals, _ = client(
            Reply({"id": 8, "name": "fraud", "modelServer": "PYTHON", "modelName": "m"})
        )
        with pytest.raises(AgentServingError, match="serves a model, not an agent"):
            evals.get_agent(8)

    def test_only_agents_are_listed(self):
        evals, _ = client(
            Reply(
                [
                    SERVING,
                    {
                        "id": 8,
                        "name": "fraud",
                        "modelServer": "PYTHON",
                        "modelName": "m",
                    },
                    {"id": 9, "name": "llm", "modelServer": "VLLM"},
                ]
            )
        )
        assert [a.name for a in evals.get_agents()] == ["support"]

    def test_the_agent_lives_behind_the_gateway(self):
        _, agent7, _ = agent()
        assert agent7.url == "https://gw/v1/g1/support"

    def test_an_agents_own_route_can_carry_a_query(self):
        # The routes an agent serves for its memory take their arguments in the
        # query string -- which subject's conversations, which fact to forget,
        # whether to include the turns it abandoned. Folded into the last
        # segment they are url-encoded with it, "?" and all, and the route stops
        # matching: every one of those calls came back 404.
        _, agent7, session = agent(Reply({"conversations": []}))

        agent7._http.agent_request(
            "GET",
            ["v1", "g1", "support", "v1", "conversations"],
            params={"subject": "alice@example.com", "limit": None},
        )

        method, url, params, _ = session.sent[0]
        assert (method, url) == ("GET", "https://gw/v1/g1/support/v1/conversations")
        # and nothing was invented for what the caller left out
        assert params == {"subject": "alice@example.com"}

    def test_chat_speaks_the_protocol_and_threads_a_conversation(self):
        _, agent7, session = agent(
            Reply({"endpoints": {"chat": "/v1/chat"}, "capabilities": ["tracing"]}),
            Reply(
                {
                    "id": "r1",
                    "conversation_id": "conv-1",
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "Order 42 "},
                            {"type": "text", "text": "shipped."},
                        ],
                    },
                    "metadata": {"trace_id": "abc"},
                }
            ),
            Reply(
                {
                    "conversation_id": "conv-1",
                    "message": {"content": []},
                    "status": "failed",
                }
            ),
        )
        reply = agent7.chat("Where is order 42?", subject="alice")
        assert session.sent[0][:2] == (
            "GET",
            "https://gw/v1/g1/support/.well-known/hopsworks-agent.json",
        )
        method, url, _, body = session.sent[1]
        assert (method, url) == ("POST", "https://gw/v1/g1/support/v1/chat")
        assert body == {
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "Where is order 42?"}],
            },
            "subject": "alice",
        }
        assert reply.text == "Order 42 shipped."
        assert reply.conversation_id == "conv-1" and reply.trace_id == "abc"
        assert not reply.failed

        follow_up = agent7.chat("And 43?", conversation_id=reply.conversation_id)
        assert session.sent[2][3]["conversation_id"] == "conv-1"
        assert follow_up.failed and follow_up.text == ""
        # the manifest is read once
        assert len(session.sent) == 3

    def test_chat_falls_back_to_the_default_route_without_a_manifest(self):
        _, agent7, session = agent(
            Reply({"errorMsg": "Not Found"}, status=404),
            Reply(
                {
                    "conversation_id": "c",
                    "message": {"content": [{"type": "text", "text": "hi"}]},
                }
            ),
        )
        assert agent7.chat("hello").text == "hi"
        assert session.sent[1][1] == "https://gw/v1/g1/support/v1/chat"

    def test_a_refusal_from_the_agent_is_an_error_with_its_message(self):
        _, agent7, _ = agent(
            Reply({"endpoints": {}}),
            Reply({"errorMsg": "guardrail: refused"}, status=422),
        )
        with pytest.raises(AgentServingError, match="guardrail: refused") as err:
            agent7.chat("do something bad")
        assert err.value.status == 422

    def test_the_serving_row_is_read_lazily_for_a_bare_handle(self):
        evals, session = client(Reply(SERVING))
        bare = Agent(evals, 7)
        assert bare.name == "support"
        assert session.sent[0][:2] == ("GET", "/serving/7")


def sse(*frames):
    """Lines of a server-sent event stream, one (event, data) per frame."""
    lines = []
    for event, data in frames:
        lines += [f"event: {event}", f"data: {json.dumps(data)}", ""]
    return lines


MANIFEST = Reply({"endpoints": {"chat": "/v1/chat", "stream": "/v1/chat/stream"}})


class TestChatStream:
    def test_the_stream_yields_text_and_keeps_tools_and_the_completed_reply(self):
        _, agent7, session = agent(
            MANIFEST,
            Reply(
                lines=sse(
                    ("tool_event", {"id": "t1", "name": "lookup", "status": "running"}),
                    ("message.delta", {"delta": {"text": "Order 42 "}}),
                    ("tool_event", {"id": "t1", "name": "lookup", "status": "done"}),
                    ("message.delta", {"delta": {"text": "shipped."}}),
                    (
                        "message.completed",
                        {
                            "conversation_id": "conv-1",
                            "message": {
                                "content": [
                                    {"type": "text", "text": "Order 42 shipped."}
                                ]
                            },
                            "metadata": {"trace_id": "abc"},
                        },
                    ),
                )
            ),
        )
        stream = agent7.chat_stream("Where is order 42?", subject="alice")
        assert isinstance(stream, ChatStream)
        method, url, _, body = session.sent[1]
        assert (method, url) == ("POST", "https://gw/v1/g1/support/v1/chat/stream")
        assert body["subject"] == "alice" and session.streamed is True

        assert list(stream) == ["Order 42 ", "shipped."]
        assert stream.text == "Order 42 shipped."
        assert [(t.name, t.status) for t in stream.tool_events] == [
            ("lookup", "running"),
            ("lookup", "done"),
        ]
        assert stream.reply.conversation_id == "conv-1"
        assert (
            stream.reply.trace_id == "abc" and stream.reply.text == "Order 42 shipped."
        )

    def test_events_give_every_frame_in_order(self):
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                lines=sse(
                    ("message.delta", {"delta": {"text": "a"}}),
                    (
                        "tool_event",
                        {"name": "search", "status": "done", "data": {"n": 2}},
                    ),
                    (
                        "message.completed",
                        {"conversation_id": "c", "message": {"content": []}},
                    ),
                )
            ),
        )
        frames = list(agent7.chat_stream("x").events())
        assert [f.type for f in frames] == ["delta", "tool", "completed"]
        assert frames[0].text == "a"
        assert frames[1].tool.name == "search" and frames[1].tool.data == {"n": 2}
        assert frames[2].reply.conversation_id == "c"

    def test_a_stream_that_stops_without_completing_is_a_failed_reply(self):
        # A dropped connection or a killed pod ends the stream after a few
        # deltas. The text that arrived is kept -- it is the agent's own words
        # -- but calling the turn completed hands an evaluator a truncated
        # answer to grade, and the conversation id the next turn would
        # continue from is not there to be had.
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                lines=sse(
                    ("message.delta", {"delta": {"text": "hel"}}),
                    ("message.delta", {"delta": {"text": "lo"}}),
                )
            ),
        )
        stream = agent7.chat_stream("x")
        assert stream.reply.text == "hello"
        assert stream.reply.status == "failed"
        assert stream.reply.metadata["incomplete_stream"] is True
        assert stream.reply.conversation_id == "" and stream.reply.trace_id is None
        assert list(stream) == []  # read once

    def test_a_json_answer_to_the_stream_endpoint_is_read_as_the_reply(self):
        # An agent with nothing to stream may answer the stream endpoint with a
        # plain JSON ChatResponse; the contract allows it and the browser
        # client handles it. Read as server-sent events those lines carry no
        # event: or data: field, so every one was skipped and the caller got an
        # empty failed reply with the answer, the conversation id and the trace
        # id all dropped.
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                {
                    "id": "response_ok",
                    "conversation_id": "conv_ok",
                    "status": "completed",
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "hello"}],
                    },
                    "metadata": {"trace_id": "abc"},
                },
                headers={"content-type": "application/json"},
            ),
        )
        reply = agent7.chat_stream("hi").reply
        assert reply.text == "hello"
        assert reply.conversation_id == "conv_ok"
        assert reply.status == "completed"
        assert reply.trace_id == "abc"

    def test_an_error_frame_raises_with_the_agents_message(self):
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                lines=sse(
                    ("message.delta", {"delta": {"text": "so far"}}),
                    (
                        "error",
                        {"code": "tool_failed", "message": "the database is down"},
                    ),
                )
            ),
        )
        stream = agent7.chat_stream("x")
        with pytest.raises(AgentServingError, match="the database is down"):
            list(stream)
        assert stream.text == "so far"

    def test_multi_line_data_comments_and_crlf_are_parsed(self):
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                lines=[
                    ": keep-alive\r",
                    "event: message.delta\r",
                    'data: {"delta":\r',
                    'data: {"text": "ok"}}\r',
                    "\r",
                    b"event: message.completed",
                    b'data: {"conversation_id": "c", "message": {"content": []}}',
                ]
            ),
        )
        stream = agent7.chat_stream("x")
        assert list(stream) == ["ok"] and stream.reply.conversation_id == "c"

    def test_the_stream_route_falls_back_to_the_default_without_a_manifest(self):
        _, agent7, session = agent(
            Reply({"errorMsg": "Not Found"}, status=404),
            Reply(lines=sse(("message.delta", {"delta": {"text": "hi"}}))),
        )
        assert list(agent7.chat_stream("x")) == ["hi"]
        assert session.sent[1][1] == "https://gw/v1/g1/support/v1/chat/stream"

    def test_a_completed_reply_carries_the_agents_tool_events(self):
        _, agent7, _ = agent(
            MANIFEST,
            Reply(
                {
                    "conversation_id": "c",
                    "message": {"content": []},
                    "metadata": {
                        "tool_events": [{"name": "lookup", "status": "failed"}]
                    },
                }
            ),
        )
        [tool] = agent7.chat("x").tool_events
        assert tool.name == "lookup" and tool.failed


class TestModels:
    def test_timestamps_read_as_the_api_sends_them(self):
        assert as_datetime("2026-09-10T08:00:00Z") == datetime(
            2026, 9, 10, 8, tzinfo=timezone.utc
        )
        assert as_datetime(1_000) == datetime.fromtimestamp(1, tz=timezone.utc)
        assert as_datetime("") is None and as_datetime("nope") is None

    def test_unknown_fields_stay_reachable_in_raw(self):
        suite = Suite.from_api({"suiteId": "s", "somethingNew": 1})
        assert suite.raw["somethingNew"] == 1


class TestCompositeIdsDoNotGoInPaths:
    """A generated id with slashes in it is a parameter, not a path segment.

    Trial ids are `run/task/version/index` and triage ids are `feedback/run`.
    Interpolated into a single-segment route they matched nothing, so every
    human review and every judge calibration answered 404 -- the two paths a
    reviewer's verdict travels.
    """

    def test_a_trial_review_sends_the_trial_id_as_a_parameter(self):
        from hopsworks_agents.eval.models import _derive_trial_id

        evals, session = client(Reply(None))
        trial_id = _derive_trial_id("run-1", "task-1", 1, 0)
        assert trial_id.count("/") == 3, "the id shape this case is about changed"

        evals.runs.review_trial(
            "run-1", trial_id, passed=True, reason="read it, it is fine"
        )

        method, path, params, body = session.sent[0]
        assert (method, path) == ("POST", "/agent-evals/runs/run-1/trials/review")
        assert params == {"trialId": trial_id}
        assert body["passed"] is True


class TestTasksThatShareAnOpeningLine:
    """Three scripted conversations that start the same way are three cases.

    A suite whose tasks all begin by giving the same email address keyed every
    one of them on that line: the comparison saw one case, reported it unchanged,
    and found none of the regressions `changes()` reported for the same run. A
    release gate reading either got a different answer.
    """

    @staticmethod
    def _tasks(suffixes):
        return [
            {
                "taskId": f"t{i}",
                "inputMessages": json.dumps(
                    [
                        {"role": "user", "content": "dana@example.com"},
                        {"role": "user", "content": tail},
                    ]
                ),
            }
            for i, tail in enumerate(suffixes)
        ]

    def _diff(self, before_status, after_status):
        tails = ["Analyze MSFT.", "Propose strategies.", "Plan execution."]
        return client(
            Reply(
                {
                    "runId": "v1",
                    "suiteId": "s",
                    "suiteVersion": 1,
                    "status": "SUCCEEDED",
                }
            ),
            Reply(
                {
                    "runId": "v2",
                    "suiteId": "s",
                    "suiteVersion": 2,
                    "status": "SUCCEEDED",
                }
            ),
            Reply(
                [{"taskId": f"t{i}", "status": s} for i, s in enumerate(before_status)]
            ),
            Reply(
                [{"taskId": f"t{i}", "status": s} for i, s in enumerate(after_status)]
            ),
            Reply(self._tasks(tails)),
            Reply(self._tasks(tails)),
        )

    def test_each_is_its_own_case(self):
        evals, _ = self._diff(["PASSED"] * 3, ["PASSED"] * 3)
        diff = evals.runs.compare("v1", "v2")
        assert len(diff.tasks) == 3, f"{diff.tasks}"
        assert len(diff.unchanged) == 3

    def test_a_regression_in_one_of_them_is_seen(self):
        evals, _ = self._diff(
            ["PASSED", "PASSED", "PASSED"], ["PASSED", "FAILED", "PASSED"]
        )
        diff = evals.runs.compare("v1", "v2")
        assert [t.task_id for t in diff.regressed] == ["t1"], f"{diff.tasks}"
        assert not diff.added and not diff.removed

    def test_the_difference_is_still_named_by_its_opening_question(self):
        evals, _ = self._diff(["PASSED"] * 3, ["PASSED", "FAILED", "PASSED"])
        diff = evals.runs.compare("v1", "v2")
        assert diff.regressed[0].question == "dana@example.com"


class TestComparingAcrossSuiteVersions:
    """A new version copies its tasks, and a copy is a new row with a new id.

    Comparing on ids alone, every carried-over case read as `added` at a version
    bump and every regression, fix and persistent failure was lost -- which is
    the moment a comparison matters most, because the version was published to
    change something.
    """

    def test_the_same_question_is_the_same_case_in_a_later_version(self):
        evals, session = client(
            Reply(
                {
                    "runId": "v1",
                    "suiteId": "s",
                    "suiteVersion": 1,
                    "status": "SUCCEEDED",
                }
            ),
            Reply(
                {
                    "runId": "v2",
                    "suiteId": "s",
                    "suiteVersion": 2,
                    "status": "SUCCEEDED",
                }
            ),
            Reply([{"taskId": "old-id", "status": "PASSED"}]),
            Reply([{"taskId": "copied-id", "status": "FAILED"}]),
            # each version's tasks: the same question under two row ids
            Reply(
                [
                    {
                        "taskId": "old-id",
                        "inputMessages": json.dumps(
                            [{"role": "user", "content": "Where is order 4182?"}]
                        ),
                    }
                ]
            ),
            Reply(
                [
                    {
                        "taskId": "copied-id",
                        "inputMessages": json.dumps(
                            [{"role": "user", "content": "Where is order 4182?"}]
                        ),
                    }
                ]
            ),
        )

        diff = evals.runs.compare("v1", "v2")

        assert [t.task_id for t in diff.regressed] == ["copied-id"], (
            f"the copied case was not matched to the one it came from: {diff.tasks}"
        )
        assert not diff.added and not diff.removed, f"{diff}"
        assert diff.regressed[0].question == "Where is order 4182?"


class TestTraceAttributes:
    """A span read back carries what was recorded on it.

    The API sends attributes as one flat list for the whole trace, keyed by span, because
    that is how they are stored. Everything that says what a span did -- its kind, the
    model, the prompt, the answer -- is in there, so a span on its own said only its name
    and when it ran, and reading a trace meant knowing about a second list.
    """

    @staticmethod
    def _trace():
        from hopsworks_agents.eval.sdk.models import Trace

        return Trace.from_api(
            {
                "spans": [
                    {"spanId": "a", "name": "invoke_agent"},
                    {"spanId": "b", "name": "chat"},
                ],
                "spanAttributes": [
                    {
                        "spanId": "a",
                        "attrKey": "openinference.span.kind",
                        "attrValue": "AGENT",
                    },
                    {
                        "spanId": "b",
                        "attrKey": "openinference.span.kind",
                        "attrValue": "LLM",
                    },
                    {"spanId": "b", "attrKey": "llm.model_name", "attrValue": "gpt-5"},
                ],
                "events": [{"eventId": 7, "spanId": "b", "name": "gen_ai.choice"}],
                "eventAttributes": [
                    {"eventId": 7, "attrKey": "role", "attrValue": "assistant"}
                ],
            }
        )

    def test_each_span_carries_its_own_attributes(self):
        spans = {s["spanId"]: s for s in self._trace().spans}
        assert spans["a"]["attributes"]["openinference.span.kind"] == "AGENT"
        assert spans["b"]["attributes"]["llm.model_name"] == "gpt-5"

    def test_a_span_with_none_gets_an_empty_dict_not_a_missing_key(self):
        from hopsworks_agents.eval.sdk.models import Trace

        trace = Trace.from_api({"spans": [{"spanId": "z"}], "spanAttributes": []})
        assert trace.spans[0]["attributes"] == {}

    def test_events_carry_theirs_too(self):
        assert self._trace().events[0]["attributes"]["role"] == "assistant"

    def test_the_flat_lists_are_still_there(self):
        trace = self._trace()
        assert len(trace.span_attributes) == 3
        assert trace.attributes_of("b")["llm.model_name"] == "gpt-5"
