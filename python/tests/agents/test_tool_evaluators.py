"""Tool-use evaluation: the metrics that need more than a list of tool names.

The theme is the one that runs through every trajectory evaluator here — an
absence of instrumentation is not a misbehaving agent. Whether a framework
writes tool arguments onto its spans is a property of the framework, so a
evaluator that cannot see them comes back ungradable rather than failing the
trial.
"""

from __future__ import annotations

import json

from hopsworks_agents.eval.evaluators import (
    ToolArgumentEvaluator,
    ToolLatencyEvaluator,
    ToolRetryEvaluator,
    UnnecessaryToolEvaluator,
)
from hopsworks_agents.eval.judges import ToolArgumentsJudge, ToolResultUsedJudge
from hopsworks_agents.eval.metrics import run_metrics
from hopsworks_agents.eval.models import Task, TraceStatus, Trial, TrialStatus


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


def task(**kwargs) -> Task:
    kwargs = _expects(kwargs)
    return Task(
        task_id="t1",
        input_messages=json.dumps([{"role": "user", "content": "cancel order 4471"}]),
        **kwargs,
    )


def trial(output: str = "done") -> Trial:
    return Trial(
        trial_id="x",
        run_id="r",
        task_id="t1",
        task_version=1,
        trial_index=0,
        deployment_id=1,
        final_output=output,
    )


def call(name: str, **kwargs) -> dict:
    base = {
        "span_id": kwargs.get("span_id", name),
        "parent_span_id": "root",
        "name": name,
        "call_id": "",
        "arguments": "",
        "result": "",
        "status": "STATUS_CODE_OK",
        "duration_ms": 10.0,
        "start_time_ns": 0,
    }
    base.update(kwargs)
    return base


def trace(*calls: dict) -> dict:
    return {
        "trace_id": "abc",
        "root_span_id": "root",
        "tool_names": [c["name"] for c in calls],
        "tool_calls": list(calls),
        "tool_error_count": sum(1 for c in calls if c["status"].endswith("ERROR")),
        "span_count": len(calls) + 1,
    }


class TestToolArguments:
    def test_arguments_carrying_the_required_keys_pass(self):
        evaluator = ToolArgumentEvaluator("cancel_order", ["order_id"])
        result = evaluator.grade(
            task(),
            trial(),
            trace(call("cancel_order", arguments='{"order_id": "4471"}')),
        )
        assert result.passed

    def test_a_missing_key_fails_and_names_it(self):
        evaluator = ToolArgumentEvaluator("cancel_order", ["order_id"])
        result = evaluator.grade(
            task(), trial(), trace(call("cancel_order", arguments='{"reason": "x"}'))
        )
        assert result.passed is False
        assert "missing order_id" in result.reason

    def test_arguments_that_are_not_json_fail_when_they_must_parse(self):
        evaluator = ToolArgumentEvaluator("cancel_order")
        result = evaluator.grade(
            task(), trial(), trace(call("cancel_order", arguments="order 4471"))
        )
        assert result.passed is False
        assert "not JSON" in result.reason

    def test_untraced_arguments_are_ungradable_not_a_failure(self):
        # whether the framework records arguments says nothing about the agent
        evaluator = ToolArgumentEvaluator("cancel_order", ["order_id"])
        result = evaluator.grade(task(), trial(), trace(call("cancel_order")))
        assert result.ungradable and result.passed is False
        assert "does not appear to trace them" in result.reason

    def test_a_tool_that_never_ran_is_ungradable_not_a_failure(self):
        # "it never called the tool" is a tool_call verdict; repeating it here
        # would make one of the two evaluators noise
        evaluator = ToolArgumentEvaluator("cancel_order", ["order_id"])
        result = evaluator.grade(task(), trial(), trace(call("lookup_order")))
        assert result.ungradable

    def test_every_call_of_the_tool_is_checked(self):
        evaluator = ToolArgumentEvaluator("lookup", ["id"])
        result = evaluator.grade(
            task(),
            trial(),
            trace(
                call("lookup", span_id="a", arguments='{"id": "1"}'),
                call("lookup", span_id="b", arguments='{"wrong": "2"}'),
            ),
        )
        assert result.passed is False
        assert result.assertions["calls_checked"] == 2

    def test_no_trace_is_ungradable(self):
        assert ToolArgumentEvaluator("x").grade(task(), trial(), None).ungradable


class TestUnnecessaryTools:
    def test_a_tool_the_task_did_not_ask_for_fails(self):
        evaluator = UnnecessaryToolEvaluator()
        result = evaluator.grade(
            task(required_tools=["cancel_order"]),
            trial(),
            trace(call("cancel_order"), call("send_email")),
        )
        assert result.passed is False
        assert "send_email" in result.reason

    def test_only_expected_tools_passes(self):
        evaluator = UnnecessaryToolEvaluator()
        result = evaluator.grade(
            task(required_tools=["cancel_order"]), trial(), trace(call("cancel_order"))
        )
        assert result.passed

    def test_an_explicit_allowlist_widens_what_is_acceptable(self):
        evaluator = UnnecessaryToolEvaluator(allowed=["cancel_order", "lookup_order"])
        result = evaluator.grade(
            task(required_tools=["cancel_order"]),
            trial(),
            trace(call("cancel_order"), call("lookup_order")),
        )
        assert result.passed

    def test_a_task_naming_no_tools_is_ungradable(self):
        # otherwise every agent that used any tool fails a check nobody set
        result = UnnecessaryToolEvaluator().grade(
            task(), trial(), trace(call("anything"))
        )
        assert result.ungradable


class TestToolRetries:
    def test_the_same_call_twice_is_a_retry(self):
        evaluator = ToolRetryEvaluator(max_retries=0)
        result = evaluator.grade(
            task(),
            trial(),
            trace(
                call("lookup", span_id="a", arguments='{"id": "1"}'),
                call("lookup", span_id="b", arguments='{"id": "1"}'),
            ),
        )
        assert result.passed is False
        assert result.assertions["matched_on"] == "identical arguments"

    def test_the_same_tool_with_different_arguments_is_not_a_retry(self):
        # looking up two different orders is two calls, not a retry
        evaluator = ToolRetryEvaluator(max_retries=0)
        result = evaluator.grade(
            task(),
            trial(),
            trace(
                call("lookup", span_id="a", arguments='{"id": "1"}'),
                call("lookup", span_id="b", arguments='{"id": "2"}'),
            ),
        )
        assert result.passed

    def test_without_arguments_it_falls_back_to_names_and_says_so(self):
        evaluator = ToolRetryEvaluator(max_retries=0)
        result = evaluator.grade(
            task(),
            trial(),
            trace(call("lookup", span_id="a"), call("lookup", span_id="b")),
        )
        assert result.passed is False
        assert "arguments not traced" in result.assertions["matched_on"]

    def test_a_budget_of_one_tolerates_a_single_retry(self):
        evaluator = ToolRetryEvaluator(max_retries=1)
        result = evaluator.grade(
            task(),
            trial(),
            trace(
                call("lookup", span_id="a", arguments="{}"),
                call("lookup", span_id="b", arguments="{}"),
            ),
        )
        assert result.passed


class TestToolLatency:
    def test_a_call_inside_the_budget_passes(self):
        result = ToolLatencyEvaluator(max_ms=500).grade(
            task(), trial(), trace(call("lookup", duration_ms=120.0))
        )
        assert result.passed

    def test_the_slowest_call_decides(self):
        result = ToolLatencyEvaluator(max_ms=500).grade(
            task(),
            trial(),
            trace(
                call("a", span_id="a", duration_ms=100.0),
                call("b", span_id="b", duration_ms=900.0),
            ),
        )
        assert result.passed is False
        assert result.assertions["slowest_tool"] == "b"

    def test_an_unfinished_span_fails_rather_than_counting_as_instant(self):
        # an unclosed span is the shape a hung tool leaves behind
        result = ToolLatencyEvaluator(max_ms=500).grade(
            task(),
            trial(),
            trace(
                call("a", span_id="a", duration_ms=10.0),
                call("b", span_id="b", duration_ms=None),
            ),
        )
        assert result.passed is False
        assert "unfinished span" in result.reason

    def test_no_timed_span_at_all_is_ungradable(self):
        result = ToolLatencyEvaluator(max_ms=500).grade(
            task(), trial(), trace(call("a", duration_ms=None))
        )
        assert result.ungradable

    def test_scoping_to_one_tool_ignores_the_others(self):
        result = ToolLatencyEvaluator(max_ms=500, tool="a").grade(
            task(),
            trial(),
            trace(
                call("a", span_id="a", duration_ms=100.0),
                call("b", span_id="b", duration_ms=9000.0),
            ),
        )
        assert result.passed


class TestToolJudges:
    def reply(self, passed: bool):
        return lambda _p: json.dumps(
            {"passed": passed, "score": 1.0 if passed else 0.0, "reason": "because"}
        )

    def test_the_argument_judge_reads_traced_arguments(self):
        evaluator = ToolArgumentsJudge(self.reply(True))
        result = evaluator.grade(
            task(),
            trial(),
            trace(call("cancel_order", arguments='{"order_id":"4471"}')),
        )
        assert result.passed

    def test_the_argument_judge_is_ungradable_without_arguments(self):
        evaluator = ToolArgumentsJudge(self.reply(True))
        assert evaluator.grade(task(), trial(), trace(call("cancel_order"))).ungradable

    def test_a_judge_returning_prose_is_ungradable_not_a_failure(self):
        evaluator = ToolArgumentsJudge(lambda _p: "looks fine to me")
        result = evaluator.grade(task(), trial(), trace(call("x", arguments="{}")))
        assert result.ungradable and result.passed is False

    def test_a_judge_that_raises_is_ungradable(self):
        def boom(_p):
            raise RuntimeError("rate limited")

        result = ToolArgumentsJudge(boom).grade(
            task(), trial(), trace(call("x", arguments="{}"))
        )
        assert result.ungradable
        assert "rate limited" in result.reason

    def test_the_result_judge_reads_tool_results(self):
        evaluator = ToolResultUsedJudge(self.reply(False))
        result = evaluator.grade(
            task(),
            trial("Your order is still open."),
            trace(call("lookup", result='{"status": "cancelled"}')),
        )
        assert result.passed is False

    def test_the_result_judge_is_ungradable_without_results(self):
        evaluator = ToolResultUsedJudge(self.reply(True))
        assert evaluator.grade(task(), trial(), trace(call("lookup"))).ungradable

    def test_the_result_judge_is_ungradable_without_an_answer(self):
        evaluator = ToolResultUsedJudge(self.reply(True))
        result = evaluator.grade(
            task(), trial(""), trace(call("lookup", result="something"))
        )
        assert result.ungradable


class TestToolErrorRate:
    def make(self, tool_errors, trace_status=TraceStatus.RECEIVED):
        return [
            Trial(
                trial_id=f"t{i}",
                run_id="r",
                task_id=f"task{i}",
                task_version=1,
                trial_index=0,
                deployment_id=1,
                status=TrialStatus.PASSED,
                trace_status=trace_status,
                tool_error_count=count,
            )
            for i, count in enumerate(tool_errors)
        ]

    def value(self, trials) -> float:
        rows = run_metrics("r", "s", 1, trials)
        return next(
            r["metric_value"] for r in rows if r["metric_name"] == "tool_error_rate"
        )

    def test_the_share_of_trials_with_a_failing_tool(self):
        assert self.value(self.make([0, 1, 0, 2])) == 0.5

    def test_trials_with_no_trace_are_excluded_rather_than_counted_clean(self):
        # counting them would report an observability gap as a healthy tool layer
        trials = self.make([1]) + self.make([None], trace_status=TraceStatus.MISSING)
        assert self.value(trials) == 1.0

    def test_no_visible_trajectory_at_all_reports_zero(self):
        assert self.value(self.make([None], trace_status=TraceStatus.MISSING)) == 0.0
