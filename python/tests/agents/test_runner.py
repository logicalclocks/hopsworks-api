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
    run_evaluators,
    verdict,
)
from hopsworks_agents.eval.metrics import (
    flaky_tasks,
    pass_all_k,
    pass_at_k,
    run_metrics,
)
from hopsworks_agents.eval.models import (
    ExecutionMode,
    Suite,
    Task,
    TraceStatus,
    Trial,
    TrialStatus,
    derive_trial_id,
)
from hopsworks_agents.eval.runner import (
    AgentResponse,
    RunnerConfig,
    SuiteRefused,
    check_deployment_supports,
    run_suite,
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
    ):
        self._answer = answer
        self._manifest = manifest if manifest is not None else CAPABLE
        self._trace = trace
        self._blocked = blocked
        self._error = error
        self._raises = raises
        self.calls: list[dict] = []

    def manifest(self):
        return self._manifest

    def call(self, prompt, *, traceparent, baggage, timeout_s):
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

    def fetch_trace(self, trace_id):
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


def go(client, s=None, evaluators=None, **config_kwargs):
    return run_suite(
        client,
        s or suite(),
        run_id="run-1",
        deployment_id=7,
        evaluators=evaluators if evaluators is not None else [ExactMatchEvaluator()],
        config=RunnerConfig(
            readiness_timeout_s=0.01, readiness_poll_s=0.001, **config_kwargs
        ),
        sleep=lambda _: None,
    )


class TestRefusals:
    """Each of these would otherwise produce results that look valid."""

    def test_refuses_a_deployment_that_cannot_correlate(self):
        # an SDK too old to continue the traceparent: every trial row would
        # point at a trace that was never created
        with pytest.raises(SuiteRefused, match="trace_correlation"):
            check_deployment_supports(suite(), {"capabilities": {}})

    def test_refuses_when_tracing_is_disabled(self):
        with pytest.raises(SuiteRefused, match="tracing is disabled"):
            check_deployment_supports(
                suite(), {"capabilities": {"trace_correlation": False}}
            )

    def test_refuses_a_sandboxed_suite_against_a_live_deployment(self):
        # the tools can still reach production systems
        with pytest.raises(SuiteRefused, match="eval_mode"):
            check_deployment_supports(
                suite(execution_mode=ExecutionMode.SANDBOXED),
                {"capabilities": {"trace_correlation": True, "eval_mode": False}},
            )

    def test_allows_a_sandboxed_suite_against_an_eval_mode_deployment(self):
        check_deployment_supports(
            suite(execution_mode=ExecutionMode.SANDBOXED), CAPABLE
        )

    def test_allows_a_sandboxed_suite_against_a_per_request_deployment(self):
        # the agent reads the run id off each turn's baggage and skips its
        # production writes for a verified run, so live traffic and a sandboxed
        # suite can share one deployment
        check_deployment_supports(
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
            check_deployment_supports(
                suite(blocks_are_success=True, execution_mode=ExecutionMode.READ_ONLY),
                CAPABLE,
            )

    def test_refuses_live_execution_mode(self):
        with pytest.raises(SuiteRefused, match="live"):
            check_deployment_supports(suite(execution_mode=ExecutionMode.LIVE), CAPABLE)

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
        assert derive_trial_id("run-1", "t1", 1, 0) == "run-1/t1/1/0"

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

    def test_infra_errors_are_excluded_from_the_pass_rate(self):
        result = go(FakeClient(raises=ConnectionError("boom")))
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in run_metrics("r", "s", 7, result.trials)
        }
        # nothing gradable happened, so the pass rate must not read as 0% agent
        # quality
        assert metrics["pass_rate"] == 0.0
        from hopsworks_agents.eval.models import gradable_trials

        assert gradable_trials(result.trials) == []


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
                        trial_id=derive_trial_id("r", task_id, 1, index),
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
        assert pass_at_k(trials) == 0.5

    def test_pass_all_k_requires_every_trial(self):
        # the reliability number: 4-of-5 is not 80% good, it is unreliable
        trials = self._trials({"a": [True, True], "b": [True, False]})
        assert pass_all_k(trials) == 0.5

    def test_pass_at_k_and_pass_all_k_differ_where_it_matters(self):
        trials = self._trials({"a": [True, False, True]})
        assert pass_at_k(trials) == 1.0
        assert pass_all_k(trials) == 0.0

    def test_flaky_tasks_are_named(self):
        trials = self._trials({"a": [True, False], "b": [True, True]})
        assert flaky_tasks(trials) == ["a"]

    def test_run_metrics_are_hand_checkable(self):
        trials = self._trials({"a": [True, True], "b": [True, False]})
        # filtered by scope: `pass_rate` is emitted per task as well as per run,
        # and flattening on the name alone silently reads the last task's value
        metrics = {
            m["metric_name"]: m["metric_value"]
            for m in run_metrics("r", "s", 7, trials)
            if m["metric_scope"] == "run"
        }
        assert metrics["pass_rate"] == 0.75  # 3 of 4 trials
        assert metrics["pass_at_k"] == 1.0  # both tasks passed once
        assert metrics["pass_all_k"] == 0.5  # only a passed every time
        assert metrics["flaky_task_count"] == 1.0

    def test_metric_rows_carry_the_scope_key(self):
        rows = run_metrics("r", "s", 7, self._trials({"a": [True]}))
        # run-scope rows carry no value; every other scope must, or two
        # categories reporting the same metric collide into one row
        for row in rows:
            if row["metric_scope"] == "run":
                assert row["metric_scope_value"] == ""
            else:
                assert row["metric_scope_value"]


class TestEvaluatorRobustness:
    def test_a_evaluator_that_raises_does_not_break_the_run(self):
        class Exploding:
            name, type, needs_trace = "boom", "custom", False

            def grade(self, task, trial, trace):
                raise ValueError("bad evaluator")

        results = run_evaluators(
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
        results = run_evaluators(
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
        assert verdict(results) is None

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
        results = run_evaluators(
            [ExactMatchEvaluator(), ContainsEvaluator(expected="five")],
            task(),
            trial,
            None,
        )
        assert verdict(results) is False


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
        result = ToolCallEvaluator().grade(
            task(required_tools=["recall"]),
            self._trial(),
            {"tool_names": '["purchase_history"]'},
        )
        assert result.passed is False
        assert result.assertions["missing_required"] == ["recall"]

    def test_forbidden_tools_must_not_be(self):
        result = ToolCallEvaluator().grade(
            task(forbidden_tools=["refund"]),
            self._trial(),
            {"tool_names": '["refund"]'},
        )
        assert result.passed is False

    def test_order_allows_extra_calls_in_between(self):
        # an agent that also checked something else has not done the wrong thing
        result = ToolOrderEvaluator().grade(
            task(required_tools=["lookup", "refund"]),
            self._trial(),
            {"tool_names": '["lookup", "audit", "refund"]'},
        )
        assert result.passed is True

    def test_order_catches_a_reversed_sequence(self):
        result = ToolOrderEvaluator().grade(
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
                if t.expects_text("exact_match")
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
