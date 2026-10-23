"""The runner: what it refuses, and how it classifies what goes wrong.

Almost every test here is about a failure path. The happy path is one call and
one comparison; the ways a run can produce results that *look* valid and are
not are the reason this component exists.
"""

import json

import pytest
from hopsworks_agents.eval.evaluators import (
    ContainsEvaluator,
    ExactMatchEvaluator,
    ToolCallEvaluator,
    ToolOrderEvaluator,
    _run_evaluators,
    _verdict,
)
from hopsworks_agents.eval.metrics import (
    _flaky_tasks,
    _pass_all_k,
    _pass_at_k,
    _run_metrics,
)
from hopsworks_agents.eval.models import (
    ExecutionMode,
    Suite,
    Task,
    TraceStatus,
    Trial,
    TrialStatus,
    _derive_trial_id,
)
from hopsworks_agents.eval.runner import (
    AgentResponse,
    RunnerConfig,
    SuiteRefused,
    _check_deployment_supports,
    _run_suite,
)


CAPABLE = {"capabilities": {"trace_correlation": True, "eval_mode": True}}


class FakeClient:
    """Stands in for a deployed agent. Records what it was sent, so the tests.

    can assert on correlation headers as well as on outcomes.
    """

    def __init__(
        self,
        *,
        answer="4",
        manifest=None,
        trace=None,
        blocked=False,
        error="",
        raises=None,
        manifest_failures=0,
    ):
        self._answer = answer
        self._manifest = manifest if manifest is not None else CAPABLE
        self._trace = trace
        self._blocked = blocked
        self._error = error
        self._raises = raises
        self._manifest_failures = manifest_failures
        self.calls: list[dict] = []

    def _fetch_manifest(self):
        # A deployment that is still coming up refuses the manifest; the runner
        # waits for it rather than reporting the restart as a broken agent.
        if self._manifest_failures > 0:
            self._manifest_failures -= 1
            raise ConnectionError("connection refused")
        return self._manifest

    def _send_prompt(self, prompt, *, traceparent, baggage, timeout_s):
        self.calls.append(
            {"prompt": prompt, "traceparent": traceparent, "baggage": baggage}
        )
        if self._raises:
            raise self._raises
        return AgentResponse(
            text=self._answer,
            trace_id=traceparent.split("-")[1],
            blocked_by_guardrail=self._blocked,
            error=self._error,
            latency_ms=100.0,
        )

    def _fetch_trace(self, trace_id):
        return self._trace


def _expects(kwargs: dict) -> dict:
    """Legacy per-field kwargs, written as the expectations they now are.

    Expectations are keyed by the check that reads them, and a check's name
    defaults to its type — so `required_tools=["x"]` is what the `tool_call` and
    `tool_order` checks expect, and an expected answer is what every check that
    reads one expects. Written once here so a test can still say the thing it
    means.
    """
    import json as _json

    expectations = dict(kwargs.pop("expectations", {}) or {})
    answer = kwargs.pop("expected_output", None)
    if answer is not None:
        for name in ("exact_match", "contains", "pairwise", "llm_judge"):
            expectations.setdefault(name, answer)
    rubric = kwargs.pop("rubric", None)
    if rubric is not None:
        expectations["llm_judge"] = rubric
    required = kwargs.pop("required_tools", None)
    forbidden = kwargs.pop("forbidden_tools", None)
    if required is not None or forbidden is not None:
        expectations["tool_call"] = _json.dumps(
            {"required": required or [], "forbidden": forbidden or []}
        )
        expectations["tool_order"] = ", ".join(required or [])
        expectations["no_unnecessary_tools"] = ", ".join(required or [])
    if expectations:
        kwargs["expectations"] = expectations
    return kwargs


def task(**kwargs):
    defaults = {
        "task_id": "t1",
        "input_messages": '[{"role": "user", "content": "what is 2+2?"}]',
        "expected_output": "4",
    }
    defaults.update(kwargs)
    return Task(**_expects(defaults))


def suite(**kwargs):
    defaults = {"suite_id": "s1", "tasks": [task()]}
    defaults.update(kwargs)
    return Suite(**defaults)


def go(client, s=None, evaluators=None, on_progress=None, **config_kwargs):
    return _run_suite(
        client,
        s or suite(),
        run_id="run-1",
        deployment_id=7,
        evaluators=evaluators if evaluators is not None else [ExactMatchEvaluator()],
        config=RunnerConfig(
            readiness_timeout_s=0.01, readiness_poll_s=0.001, **config_kwargs
        ),
        sleep=lambda _: None,
        on_progress=on_progress,
    )


class TestRefusals:
    """Each of these would otherwise produce results that look valid."""

    def test_refuses_a_deployment_that_cannot_correlate(self):
        # an SDK too old to continue the traceparent: every trial row would
        # point at a trace that was never created
        with pytest.raises(SuiteRefused, match="trace_correlation"):
            _check_deployment_supports(suite(), {"capabilities": {}})

    def test_refuses_when_tracing_is_disabled(self):
        with pytest.raises(SuiteRefused, match="tracing is disabled"):
            _check_deployment_supports(
                suite(), {"capabilities": {"trace_correlation": False}}
            )

    def test_refuses_a_sandboxed_suite_against_a_live_deployment(self):
        # the tools can still reach production systems
        with pytest.raises(SuiteRefused, match="eval_mode"):
            _check_deployment_supports(
                suite(execution_mode=ExecutionMode.SANDBOXED),
                {"capabilities": {"trace_correlation": True, "eval_mode": False}},
            )

    def test_allows_a_sandboxed_suite_against_an_eval_mode_deployment(self):
        _check_deployment_supports(
            suite(execution_mode=ExecutionMode.SANDBOXED), CAPABLE
        )

    def test_allows_a_sandboxed_suite_against_a_per_request_deployment(self):
        # the agent reads the run id off each turn's baggage and skips its
        # production writes for a verified run, so live traffic and a sandboxed
        # suite can share one deployment
        _check_deployment_supports(
            suite(execution_mode=ExecutionMode.SANDBOXED),
            {
                "capabilities": {
                    "trace_correlation": True,
                    "eval_mode": False,
                    "eval_per_request": True,
                }
            },
        )

    def test_refuses_a_safety_suite_that_is_not_sandboxed(self):
        # safety suites contain injection and exfiltration attempts by
        # construction; read_only is not a strong enough claim
        with pytest.raises(SuiteRefused, match="sandboxed"):
            _check_deployment_supports(
                suite(blocks_are_success=True, execution_mode=ExecutionMode.READ_ONLY),
                CAPABLE,
            )

    def test_refuses_live_execution_mode(self):
        with pytest.raises(SuiteRefused, match="live"):
            _check_deployment_supports(
                suite(execution_mode=ExecutionMode.LIVE), CAPABLE
            )

    def test_refuses_task_types_it_cannot_run(self):
        with pytest.raises(SuiteRefused, match="handwritten"):
            go(FakeClient(), suite(tasks=[task(task_type="handwritten")]))

    def test_refuses_several_turns_typed_as_one(self):
        # what this replaces: prompt returned the last user message, so a task
        # like this ran its final turn alone and reported a pass rate for a
        # conversation that never happened
        script = json.dumps(
            [
                {"role": "user", "content": "hello"},
                {"role": "user", "content": "and again"},
            ]
        )
        with pytest.raises(SuiteRefused, match="several user turns"):
            go(
                FakeClient(),
                suite(tasks=[task(input_messages=script, task_type="single_turn")]),
            )


class TestCorrelation:
    def test_sends_a_traceparent_and_eval_baggage(self):
        client = FakeClient()
        go(client)
        sent = client.calls[0]
        assert sent["traceparent"].startswith("00-")
        assert "hopsworks.eval.run_id=run-1" in sent["baggage"]
        assert "hopsworks.eval.trial_id=run-1/t1/1/0" in sent["baggage"]

    def test_the_trial_keeps_its_trace_id_even_when_the_call_fails(self):
        # the reason the runner generates the id instead of reading it back
        client = FakeClient(raises=RuntimeError("connection refused"))
        result = go(client)
        assert result.trials[0].trace_id
        assert result.trials[0].status is TrialStatus.INFRA_ERROR


class TestIdempotency:
    def test_trial_ids_are_deterministic(self):
        assert _derive_trial_id("run-1", "t1", 1, 0) == "run-1/t1/1/0"

    def test_a_rerun_produces_the_same_trial_ids(self):
        # a crashed runner that retries must overwrite rows, not append; random
        # ids would turn every hiccup into duplicated results and a wrong pass
        # rate
        first = go(FakeClient(), n_trials=3)
        second = go(FakeClient(), n_trials=3)
        assert [t.trial_id for t in first.trials] == [t.trial_id for t in second.trials]

    def test_trials_of_one_task_have_distinct_ids(self):
        result = go(FakeClient(), n_trials=3)
        assert len({t.trial_id for t in result.trials}) == 3


class TestFailureClassification:
    def test_an_unreachable_agent_is_infra_not_agent_error(self):
        # sending someone to debug a prompt over a network failure is the
        # failure mode this separation prevents
        result = go(FakeClient(raises=ConnectionError("boom")))
        assert result.trials[0].status is TrialStatus.INFRA_ERROR

    def test_a_timeout_is_classified_as_one(self):
        result = go(FakeClient(error="request timeout after 120s"))
        assert result.trials[0].status is TrialStatus.TIMEOUT

    def test_an_agent_that_crashes_is_the_agent_failing(self):
        # The SDK answers 500 when a handler raises. Filed as INFRA_ERROR it was
        # dropped from the denominator, so an agent that crashed on two of three
        # tasks reported a pass rate of 1.0 and passed its gate.
        result = go(FakeClient(error="agent returned 500: KeyError('customer')"))
        assert result.trials[0].status is TrialStatus.AGENT_ERROR
        from hopsworks_agents.eval.models import _gradable_trials

        assert len(_gradable_trials(result.trials)) == 1

    def test_an_agent_reporting_its_own_failure_counts_too(self):
        # status: "failed" in the body, with HTTP 200 around it
        result = go(FakeClient(error="agent reported failure"))
        assert result.trials[0].status is TrialStatus.AGENT_ERROR

    def test_the_gateway_refusing_is_still_infra(self):
        # 503 is Knative saying it could not reach the revision at all, which
        # says nothing about the agent's code
        result = go(FakeClient(error="agent returned 503: no healthy upstream"))
        assert result.trials[0].status is TrialStatus.INFRA_ERROR

    def test_a_request_the_agent_refused_is_infra(self):
        # a 4xx is the harness sending something wrong, not the agent breaking
        result = go(FakeClient(error="agent returned 422: bad payload"))
        assert result.trials[0].status is TrialStatus.INFRA_ERROR

    def test_infra_errors_are_excluded_from_the_pass_rate(self):
        result = go(FakeClient(raises=ConnectionError("boom")))
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "s", 7, result.trials)
        }
        # nothing gradable happened, so the pass rate must not read as 0% agent
        # quality
        assert metrics["pass_rate"] == 0.0
        from hopsworks_agents.eval.models import _gradable_trials

        assert _gradable_trials(result.trials) == []


class TestWhenTheHarnessItselfBreaks:
    def test_one_broken_trial_does_not_throw_away_the_run(self, monkeypatch):
        # Everything that can fail on the agent's behalf is already caught and
        # classified; this is the harness breaking on one trial. Raised through
        # the pool it lost every other trial of the run as well -- the whole
        # suite's work, written nowhere, because of one bug.
        from hopsworks_agents.eval import runner as runner_module

        real = runner_module._run_trial

        def explode(client, suite, task, trial_index, *args, **kwargs):
            if trial_index == 0:
                raise RuntimeError("harness bug")
            return real(client, suite, task, trial_index, *args, **kwargs)

        monkeypatch.setattr(runner_module, "_run_trial", explode)

        result = go(FakeClient(), n_trials=2)

        assert len(result.trials) == 2
        broken = [t for t in result.trials if t.status is TrialStatus.INFRA_ERROR]
        assert len(broken) == 1
        assert "harness bug" in broken[0].error_message
        assert broken[0].trial_id == "run-1/t1/1/0"


class TestResultsAsTheyArrive:
    """A killed pod used to leave nothing, however far the run had got."""

    def test_finished_trials_are_handed_over_while_the_run_is_going(self):
        batches = []
        go(
            FakeClient(),
            s=suite(tasks=[task(task_id=f"t{i}") for i in range(5)]),
            n_trials=2,
            write_batch=3,
            on_progress=batches.append,
        )
        # ten trials in batches of three, so the last four rows are not the only
        # thing a run that died half way through would have left
        assert [len(b) for b in batches] == [3, 3, 3, 1]
        assert sum(len(b) for b in batches) == 10

    def test_a_write_that_fails_does_not_fail_the_run(self):
        def explode(_trials):
            raise RuntimeError("the store is down")

        result = go(
            FakeClient(trace={"root_span_id": "a", "tool_names": "[]"}),
            on_progress=explode,
        )
        assert result.status == "SUCCEEDED"
        assert len(result.trials) == 1

    def test_nothing_is_handed_over_when_nobody_asked(self):
        # the default stays what it was: one write at the end
        healthy = FakeClient(trace={"root_span_id": "a", "tool_names": "[]"})
        assert go(healthy).status == "SUCCEEDED"


class TestGuardrails:
    def test_a_block_passes_a_safety_suite(self):
        # the block is the desired outcome there
        result = go(
            FakeClient(blocked=True),
            suite(blocks_are_success=True, execution_mode=ExecutionMode.SANDBOXED),
        )
        assert result.trials[0].status is TrialStatus.PASSED

    def test_a_block_fails_a_capability_suite_as_an_over_refusal(self):
        # measuring only guardrail recall is the classic mistake: guardrails
        # look effective while quietly degrading the product
        result = go(FakeClient(blocked=True), suite())
        assert result.trials[0].status is TrialStatus.BLOCKED_BY_GUARDRAIL

    def test_a_block_is_never_folded_into_agent_error(self):
        result = go(FakeClient(blocked=True), suite())
        assert result.trials[0].status is not TrialStatus.AGENT_ERROR

    def test_a_safety_suite_still_reports_the_blocks_it_provoked(self):
        # The block is the pass there, so the status stops saying one happened
        # -- and the block rate, which is meant to be the raw count, read 0% in
        # the one suite type built to trigger guardrails.
        result = go(
            FakeClient(blocked=True),
            suite(blocks_are_success=True, execution_mode=ExecutionMode.SANDBOXED),
        )
        assert result.trials[0].guardrail_blocked
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "s", 7, result.trials, blocks_are_success=True)
        }
        assert metrics["guardrail_block_rate"] == 1.0
        # and it is not read as timidity: refusing an attack is the point
        assert metrics["over_refusal_rate"] == 0.0
        assert metrics["safety_violation_rate"] == 0.0

    def test_the_default_judge_follows_the_deployments_one_setting(self, monkeypatch):
        # A project that judges with one model should say so once rather than
        # in every check. A check naming its own still wins; this is the
        # default it falls back to.
        from hopsworks_agents.eval.run_job import _judge_for

        monkeypatch.setenv("EVAL_JUDGE_API_KEY", "k")
        monkeypatch.delenv("EVAL_JUDGE_MODEL", raising=False)
        judge = _judge_for("openai", "gpt-5.6-terra")
        assert judge is not None
        assert judge.config.provider == "openai"
        assert judge.config.model == "gpt-5.6-terra"

        # and without one, what it has always been
        assert _judge_for().config.provider == "anthropic"

    def test_an_unknown_judge_provider_falls_back_rather_than_failing(
        self, monkeypatch
    ):
        from hopsworks_agents.eval.run_job import _judge_for

        monkeypatch.setenv("EVAL_JUDGE_API_KEY", "k")
        assert _judge_for("not-a-provider").config.provider == "anthropic"

    def test_no_key_means_no_default_judge_rather_than_a_failed_run(self, monkeypatch):
        from hopsworks_agents.eval.run_job import _judge_for

        monkeypatch.delenv("EVAL_JUDGE_API_KEY", raising=False)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert _judge_for() is None

    def test_a_sample_records_what_its_number_is_a_number_of(self):
        # "500 passed 80%" reads as the hour's verdict; the hour held twelve
        # thousand conversations and this was five hundred of them.
        result = go(FakeClient(), suite())
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "", 7, result.trials, window_size=12000)
        }
        assert metrics["window_conversations"] == 12000.0
        assert metrics["sampled_fraction"] == len(result.trials) / 12000

    def test_a_suite_run_has_no_window_to_report(self):
        metrics = {
            m["metric_name"] for m in _run_metrics("r", "s", 7, go(FakeClient()).trials)
        }
        assert "window_conversations" not in metrics
        assert "sampled_fraction" not in metrics

    def test_a_capability_suite_counts_the_same_block_as_over_refusal(self):
        result = go(FakeClient(blocked=True), suite())
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "s", 7, result.trials)
        }
        assert metrics["guardrail_block_rate"] == 1.0
        assert metrics["over_refusal_rate"] == 1.0


class TestWhatCountsAsAMissingTrace:
    """A trial that never waited for a trace has not lost one.

    More than half the trials missing a trace fails the run, which is right
    when the observability pipeline is broken. But a trial that returns before
    the trace is awaited -- blocked by a guardrail, an agent error, a timeout,
    an infrastructure failure -- never asked for one. Counting those failed the
    runs where nothing was missing, and since the gate reads the newest
    SUCCEEDED run, a regressed deployment kept showing an older green one.
    """

    def test_a_safety_suite_that_blocks_everything_succeeds(self):
        # every attack correctly refused: every trial passed, and the run used
        # to come back FAILED for traces nobody waited for
        result = go(
            FakeClient(blocked=True),
            suite(blocks_are_success=True, execution_mode=ExecutionMode.SANDBOXED),
        )

        assert [t.status for t in result.trials] == [TrialStatus.PASSED]
        assert all(t.trace_status is TraceStatus.NOT_FETCHED for t in result.trials)
        assert result.status == "SUCCEEDED"
        assert result.error_message == ""

    def test_the_metric_is_over_the_trials_that_waited(self):
        result = go(
            FakeClient(blocked=True),
            suite(blocks_are_success=True, execution_mode=ExecutionMode.SANDBOXED),
        )
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "s", 7, result.trials)
        }
        # no trial waited for a trace, so none is missing one
        assert metrics["trace_missing_rate"] == 0.0

    def test_a_genuinely_missing_trace_still_fails_the_run(self):
        result = go(FakeClient(trace=None), evaluators=[ToolCallEvaluator()])

        assert all(t.trace_status is TraceStatus.MISSING for t in result.trials)
        assert result.status == "FAILED"
        # and says so on the row, rather than sending someone to the pod log
        assert "never got one" in result.error_message


class TestTraceReadiness:
    def test_final_answer_evaluators_still_run_without_a_trace(self):
        result = go(FakeClient(trace=None), evaluators=[ExactMatchEvaluator()])
        trial = result.trials[0]
        assert trial.trace_status is TraceStatus.MISSING
        assert trial.status is TrialStatus.PASSED  # the answer was still right

    def test_trajectory_evaluators_go_ungradable_not_failing(self):
        # scoring zero would report a broken observability pipeline as a broken
        # agent, and block a good deployment
        result = go(FakeClient(trace=None), evaluators=[ToolCallEvaluator()])
        trial = result.trials[0]
        assert trial.evaluator_results[0].ungradable is True
        assert trial.status is TrialStatus.TRACE_MISSING

    def test_a_trace_without_a_root_span_is_partial(self):
        result = go(FakeClient(trace={"tool_names": "[]"}))
        assert result.trials[0].trace_status is TraceStatus.PARTIAL

    def test_the_root_span_is_waited_for_before_a_trajectory_is_graded(self):
        # The sidecar batches, so a trace is readable from its first child
        # onwards and the turn span is usually last. Returning on the first
        # readable version handed the trajectory checks half a trace, and "the
        # agent did not call the tool" was recorded as a definite failure.
        class LateRoot(FakeClient):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.reads = 0

            def _fetch_trace(self, trace_id):
                self.reads += 1
                if self.reads < 3:
                    return {"tool_names": "[]"}
                return {"root_span_id": "a", "tool_names": '["search"]'}

        client = LateRoot()
        result = go(client, evaluators=[ToolCallEvaluator()])
        trial = result.trials[0]
        assert trial.trace_status is TraceStatus.RECEIVED
        assert client.reads >= 3

    def test_the_row_points_at_the_trace_that_was_graded(self):
        # An agent that makes its own trace instead of continuing the one the
        # traceparent named had that trace graded while the trial row kept the
        # id the runner generated -- a row naming a trace nobody can open.
        class OwnTrace(FakeClient):
            def _send_prompt(self, prompt, *, traceparent, baggage, timeout_s):
                response = super()._send_prompt(
                    prompt,
                    traceparent=traceparent,
                    baggage=baggage,
                    timeout_s=timeout_s,
                )
                response.trace_id = "ffffffffffffffffffffffffffffffff"
                return response

            def _fetch_trace(self, trace_id):
                self.asked = trace_id
                return {"root_span_id": "a", "tool_names": "[]"}

        client = OwnTrace()
        trial = go(client).trials[0]
        assert client.asked == "ffffffffffffffffffffffffffffffff"
        assert trial.trace_id == "ffffffffffffffffffffffffffffffff"

    def test_a_trace_that_never_completes_is_graded_as_partial(self):
        # what is there when the time runs out is still worth grading, with
        # "partial" on the row saying what it was graded against
        result = go(FakeClient(trace={"tool_names": "[]"}))
        assert result.trials[0].trace_status is TraceStatus.PARTIAL

    def test_a_run_that_mostly_loses_traces_fails_itself(self):
        # the pipeline is broken, not the agent; passing would publish a pass
        # rate computed from answers while claiming to have judged trajectories
        result = go(
            FakeClient(trace=None), evaluators=[ToolCallEvaluator()], n_trials=2
        )
        assert result.status == "FAILED"

    def test_a_healthy_run_succeeds(self):
        result = go(FakeClient(trace={"root_span_id": "a", "tool_names": "[]"}))
        assert result.status == "SUCCEEDED"


class TestConcurrency:
    def test_every_task_and_trial_is_executed(self):
        client = FakeClient()
        result = go(
            client, suite(tasks=[task(task_id="a"), task(task_id="b")]), n_trials=3
        )
        assert len(result.trials) == 6
        assert len(client.calls) == 6

    def test_results_come_back_in_a_stable_order(self):
        result = go(
            FakeClient(),
            suite(tasks=[task(task_id="b"), task(task_id="a")]),
            n_trials=2,
            max_concurrency=4,
        )
        assert [(t.task_id, t.trial_index) for t in result.trials] == [
            ("a", 0),
            ("a", 1),
            ("b", 0),
            ("b", 1),
        ]


class TestMetrics:
    def _trials(self, outcomes: dict[str, list[bool]]) -> list[Trial]:
        trials = []
        for task_id, results in outcomes.items():
            for index, ok in enumerate(results):
                trials.append(
                    Trial(
                        trial_id=_derive_trial_id("r", task_id, 1, index),
                        run_id="r",
                        task_id=task_id,
                        task_version=1,
                        trial_index=index,
                        deployment_id=7,
                        status=TrialStatus.PASSED if ok else TrialStatus.FAILED,
                        trace_status=TraceStatus.RECEIVED,
                        latency_ms=100.0,
                    )
                )
        return trials

    def test_pass_at_k_counts_a_task_that_ever_succeeded(self):
        trials = self._trials({"a": [False, True], "b": [False, False]})
        assert _pass_at_k(trials) == 0.5

    def test_pass_all_k_requires_every_trial(self):
        # the reliability number: 4-of-5 is not 80% good, it is unreliable
        trials = self._trials({"a": [True, True], "b": [True, False]})
        assert _pass_all_k(trials) == 0.5

    def test_pass_at_k_and_pass_all_k_differ_where_it_matters(self):
        trials = self._trials({"a": [True, False, True]})
        assert _pass_at_k(trials) == 1.0
        assert _pass_all_k(trials) == 0.0

    def test_flaky_tasks_are_named(self):
        trials = self._trials({"a": [True, False], "b": [True, True]})
        assert _flaky_tasks(trials) == ["a"]

    def test_run_metrics_are_hand_checkable(self):
        trials = self._trials({"a": [True, True], "b": [True, False]})
        # filtered by scope: `pass_rate` is emitted per task as well as per run,
        # and flattening on the name alone silently reads the last task's value
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in _run_metrics("r", "s", 7, trials)
            if m["metric_scope"] == "run"
        }
        assert metrics["pass_rate"] == 0.75  # 3 of 4 trials
        assert metrics["pass_at_k"] == 1.0  # both tasks passed once
        assert metrics["pass_all_k"] == 0.5  # only a passed every time
        assert metrics["flaky_task_count"] == 1.0

    def test_metric_rows_carry_the_scope_key(self):
        rows = _run_metrics("r", "s", 7, self._trials({"a": [True]}))
        # run-scope rows carry no value; every other scope must, or two
        # categories reporting the same metric collide into one row
        for row in rows:
            if row["metric_scope"] == "run":
                assert row["metric_scope_value"] == ""
            else:
                assert row["metric_scope_value"]


class TestATaskThatExpectsNothing:
    """A check with nothing to grade against reports that, rather than a verdict.

    The design says so and the backend gate keeps such a task out of a suite --
    but a suite defined in code has no gate, and these were answering anyway:
    `contains` passed because "" is in every string, `tool_order` passed
    because an empty order is a subsequence of anything, and `exact_match`
    failed, blaming the agent for a task that never said what right was.
    """

    def _trial(self, answer="4"):
        return Trial(
            trial_id="x",
            run_id="r",
            task_id="t1",
            task_version=1,
            trial_index=0,
            deployment_id=7,
            final_output=answer,
        )

    def test_contains_says_so_instead_of_passing(self):
        blank = Task(task_id="t1", input_messages="[]", expectations={})
        result = ContainsEvaluator()._grade(blank, self._trial(), None)
        assert result.ungradable is True
        assert result.passed is None or result.passed is False

    def test_exact_match_says_so_instead_of_failing(self):
        blank = Task(task_id="t1", input_messages="[]", expectations={})
        result = ExactMatchEvaluator()._grade(blank, self._trial(), None)
        assert result.ungradable is True

    def test_tool_order_says_so_instead_of_passing(self):
        blank = Task(task_id="t1", input_messages="[]", expectations={})
        result = ToolOrderEvaluator()._grade(
            blank, self._trial(), {"tool_names": '["lookup"]'}
        )
        assert result.ungradable is True


class TestEvaluatorRobustness:
    def test_a_evaluator_that_raises_does_not_break_the_run(self):
        class Exploding:
            name, type, needs_trace = "boom", "custom", False

            def _grade(self, task, trial, trace):
                raise ValueError("bad evaluator")

        results = _run_evaluators(
            [Exploding()],
            task(),
            Trial(
                trial_id="x",
                run_id="r",
                task_id="t1",
                task_version=1,
                trial_index=0,
                deployment_id=7,
            ),
            None,
        )
        assert results[0].ungradable is True
        assert "bad evaluator" in results[0].reason

    def test_verdict_is_none_when_nothing_was_gradable(self):
        # not the same as failing: the trial says nothing either way
        results = _run_evaluators(
            [ToolCallEvaluator()],
            task(),
            Trial(
                trial_id="x",
                run_id="r",
                task_id="t1",
                task_version=1,
                trial_index=0,
                deployment_id=7,
            ),
            None,
        )
        assert _verdict(results) is None

    def test_every_gradable_evaluator_must_pass(self):
        trial = Trial(
            trial_id="x",
            run_id="r",
            task_id="t1",
            task_version=1,
            trial_index=0,
            deployment_id=7,
            final_output="4",
        )
        results = _run_evaluators(
            [ExactMatchEvaluator(), ContainsEvaluator(expected="five")],
            task(),
            trial,
            None,
        )
        assert _verdict(results) is False


class TestToolEvaluators:
    def _trial(self):
        return Trial(
            trial_id="x",
            run_id="r",
            task_id="t1",
            task_version=1,
            trial_index=0,
            deployment_id=7,
            final_output="ok",
        )

    def test_required_tools_must_be_called(self):
        result = ToolCallEvaluator()._grade(
            task(required_tools=["recall"]),
            self._trial(),
            {"tool_names": '["purchase_history"]'},
        )
        assert result.passed is False
        assert result.assertions["missing_required"] == ["recall"]

    def test_forbidden_tools_must_not_be(self):
        result = ToolCallEvaluator()._grade(
            task(forbidden_tools=["refund"]),
            self._trial(),
            {"tool_names": '["refund"]'},
        )
        assert result.passed is False

    def test_order_allows_extra_calls_in_between(self):
        # an agent that also checked something else has not done the wrong thing
        result = ToolOrderEvaluator()._grade(
            task(required_tools=["lookup", "refund"]),
            self._trial(),
            {"tool_names": '["lookup", "audit", "refund"]'},
        )
        assert result.passed is True

    def test_order_catches_a_reversed_sequence(self):
        result = ToolOrderEvaluator()._grade(
            task(required_tools=["lookup", "refund"]),
            self._trial(),
            {"tool_names": '["refund", "lookup"]'},
        )
        assert result.passed is False


class TestPerTaskEvaluators:
    """Which evaluators apply depends on what each task declares.

    One fixed list for a whole suite either judges a task on tools it never
    claimed to use, or judges nothing at all — and a run that graded nothing
    reports cleanly while saying nothing, which is the worst of both.
    """

    def test_each_task_gets_its_own_evaluators(self):
        answered = suite(
            tasks=[
                task(task_id="answer", expected_output="4"),
                task(
                    task_id="tools",
                    expected_output="",
                    expectations={"tool_call": "search"},
                ),
            ]
        )

        def per_task(t):
            return (
                [ExactMatchEvaluator()]
                if t._expects_text("exact_match")
                else [ToolCallEvaluator()]
            )

        result = go(
            FakeClient(trace={"root_span_id": "a", "tool_names": '["search"]'}),
            answered,
            evaluators=per_task,
        )

        by_task = {
            t.task_id: [g.evaluator_type for g in t.evaluator_results]
            for t in result.trials
        }
        assert by_task["answer"] == ["exact_match"]
        assert by_task["tools"] == ["tool_call"]

    def test_a_plain_list_still_applies_to_every_task(self):
        # the simple case keeps working; per-task is an option, not a burden
        result = go(
            FakeClient(),
            suite(tasks=[task(task_id="a"), task(task_id="b")]),
            evaluators=[ExactMatchEvaluator()],
        )
        assert all(len(t.evaluator_results) == 1 for t in result.trials)

    def test_a_task_declaring_nothing_is_ungradable_not_passing(self):
        # silence must not read as success
        result = go(
            FakeClient(), suite(tasks=[task(task_id="empty")]), evaluators=lambda _t: []
        )
        assert result.trials[0].status is TrialStatus.TRACE_MISSING
        assert result.trials[0].evaluator_results == []


class TestPassPolicyAndReview:
    """The suite's policy decides the trial, and a person can hold it open."""

    def test_the_suite_policy_reaches_the_verdict(self):
        # `all` would fail this trial: the exact match passes, the second does not
        from hopsworks_agents.eval.evaluators import RegexEvaluator
        from hopsworks_agents.eval.models import PassPolicy

        evaluators = [ExactMatchEvaluator(), RegexEvaluator(r"never matches this")]

        strict = go(FakeClient(answer="4"), evaluators=evaluators)
        assert strict.trials[0].status is TrialStatus.FAILED

        lenient = go(
            FakeClient(answer="4"),
            s=suite(pass_policy=PassPolicy.ANY),
            evaluators=evaluators,
        )
        assert lenient.trials[0].status is TrialStatus.PASSED

    def test_a_threshold_policy_reads_the_mean_score(self):
        from hopsworks_agents.eval.models import PassPolicy

        result = go(
            FakeClient(answer="4"),
            s=suite(pass_policy=PassPolicy.THRESHOLD, pass_threshold=0.4),
            evaluators=[ExactMatchEvaluator(), ContainsEvaluator(expected="nowhere")],
        )
        assert result.trials[0].status is TrialStatus.PASSED

    def test_a_task_awaiting_review_is_neither_passed_nor_failed(self):
        # the other evaluators agreeing is not the judgement the task asked for
        from hopsworks_agents.eval.evaluators import HumanReviewEvaluator

        result = go(
            FakeClient(answer="4"),
            evaluators=[
                ExactMatchEvaluator(),
                HumanReviewEvaluator("is the tone right?"),
            ],
        )
        assert result.trials[0].status is TrialStatus.AWAITING_REVIEW

    def test_a_check_that_could_not_run_is_not_answered_by_the_others(self):
        """A judge with no key used to leave the run, and the rest reported a pass.

        That is a suite of `llm_judge` + `no_tool_error` -- the shape the
        standing regression suite ships with -- grading nothing about
        correctness and calling a wrong answer PASSED, with nothing on the row
        saying the judgement never happened. EVALUATOR_ERROR is the harness
        failing, which keeps the trial out of every pass denominator.
        """
        from hopsworks_agents.eval.evaluator_spec import _evaluators_from_spec

        # No judge_completer and no key: exactly what a project without one has.
        evaluators = _evaluators_from_spec(
            [
                {"type": "llm_judge", "name": "matches_correction"},
                {"type": "no_tool_error", "name": "no_tool_errored"},
            ]
        )

        result = go(FakeClient(answer="not the answer"), evaluators=evaluators)

        trial = result.trials[0]
        assert trial.status is TrialStatus.EVALUATOR_ERROR, (
            f"a trial whose judge never ran reported {trial.status}"
        )
        assert trial.error_type == "EVALUATOR_SKIPPED"
        assert "judge" in (trial.error_message or "")
        # And the check is on the row, so a reader can see which one was skipped.
        skipped = [r for r in trial.evaluator_results if r.assertions.get("skipped")]
        assert [r.evaluator_name for r in skipped] == ["matches_correction"]

    def test_a_judge_that_errors_is_not_answered_by_the_others_either(self):
        """A key that expires mid-run is the same hole as a key that is absent.

        SkippedEvaluator covers the judge that could not be built. A judge that
        was built and then failed -- an expired key, an exhausted quota, a
        wrong model name, a provider outage -- returned an ungradable result,
        which `verdict` drops, so the deterministic checks decided the trial
        and a wrong answer came back PASSED in a run reported SUCCEEDED with
        pass_rate 1.0.
        """
        from hopsworks_agents.eval.judges import LlmJudgeEvaluator

        def dead_key(prompt):
            raise RuntimeError("401 invalid x-api-key")

        # the judge plus one deterministic check that needs no trace: exactly
        # the shape that used to pass a wrong answer on the second check alone
        judge = LlmJudgeEvaluator(dead_key, name="is_it_right")
        result = go(
            FakeClient(answer="not the answer"),
            suite(tasks=[task(expectations={"is_it_right": "a correct answer"})]),
            evaluators=[judge, ExactMatchEvaluator()],
        )

        trial = result.trials[0]
        assert trial.status is TrialStatus.EVALUATOR_ERROR, (
            f"a trial whose judge errored reported {trial.status}"
        )
        assert "401" in (trial.error_message or "")

    def test_a_judge_reply_nobody_can_parse_is_treated_the_same(self):
        from hopsworks_agents.eval.judges import LlmJudgeEvaluator

        judge = LlmJudgeEvaluator(
            lambda prompt: "I think it is fine!", name="is_it_right"
        )
        result = go(
            FakeClient(answer="not the answer"),
            suite(tasks=[task(expectations={"is_it_right": "a correct answer"})]),
            evaluators=[judge, ExactMatchEvaluator()],
        )

        assert result.trials[0].status is TrialStatus.EVALUATOR_ERROR

    def test_the_deferred_result_is_kept_so_a_reviewer_can_see_the_prompt(self):
        from hopsworks_agents.eval.evaluators import HumanReviewEvaluator

        result = go(
            FakeClient(answer="4"),
            evaluators=[HumanReviewEvaluator("is the tone right?")],
        )
        pending = [
            r
            for r in result.trials[0].evaluator_results
            if r.assertions.get("awaiting_review")
        ]
        assert len(pending) == 1
        assert "tone" in pending[0].reason


class TestWaitingForTheDeployment:
    """A run can start while its deployment is still restarting.

    A canary fired by an update is exactly that: the update is what restarted it.
    """

    def test_it_waits_for_a_deployment_that_is_still_coming_up(self):
        client = FakeClient(manifest_failures=3)
        slept: list[float] = []
        result = _run_suite(
            client,
            suite(),
            run_id="r1",
            deployment_id=7,
            evaluators=[],
            # the trace poll is not what this is about, so it gives up at once
            config=RunnerConfig(readiness_timeout_s=0.0, deployment_ready_poll_s=0.0),
            sleep=slept.append,
        )
        assert result.trials
        # it waited rather than failing on the first refusal
        assert len(slept) >= 3

    def test_it_gives_up_with_a_reason_rather_than_grading_zeros(self):
        client = FakeClient(manifest_failures=10_000)
        config = RunnerConfig(
            deployment_ready_timeout_s=0.0, deployment_ready_poll_s=0.0
        )
        with pytest.raises(SuiteRefused, match="did not answer"):
            _run_suite(
                client,
                suite(),
                run_id="r1",
                deployment_id=7,
                evaluators=[],
                config=config,
                sleep=lambda _s: None,
            )


class TestATaskIsAskedAsWhoeverItNames:
    """A promoted task replays the subject and context of the turn it came from.

    A memory failure only happens to someone the agent has memory of. Asked by a
    stranger, its regression passes, so the regression suite could never hold the
    one kind of failure memory exists to prevent.
    """

    class Recording(FakeClient):
        def _send_prompt(self, prompt, *, traceparent, baggage, timeout_s, **identity):
            self.identity = getattr(self, "identity", [])
            self.identity.append(identity)
            return super()._send_prompt(
                prompt, traceparent=traceparent, baggage=baggage, timeout_s=timeout_s
            )

    def test_the_subject_and_context_ride_on_the_request(self):
        client = self.Recording()
        go(
            client,
            suite(tasks=[task(subject="marcus@example.com", context={"plan": "pro"})]),
        )
        assert client.identity == [
            {"subject": "marcus@example.com", "context": {"plan": "pro"}}
        ]

    def test_every_turn_of_a_scripted_conversation_carries_them(self):
        client = self.Recording()
        script = json.dumps(
            [
                {"role": "user", "content": "hi"},
                {"role": "user", "content": "what do you know about me?"},
            ]
        )
        go(
            client,
            suite(
                tasks=[
                    task(
                        input_messages=script,
                        task_type="multi_turn",
                        subject="marcus@example.com",
                    )
                ]
            ),
        )
        assert [i.get("subject") for i in client.identity] == ["marcus@example.com"] * 2

    def test_an_anonymous_task_sends_neither(self):
        client = self.Recording()
        go(client, suite(tasks=[task()]))
        assert client.identity == [{}]
