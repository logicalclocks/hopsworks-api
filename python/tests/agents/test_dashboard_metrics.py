"""Metrics at every scope a dashboard needs, and the two rates that were wrong.

`guardrail_block_rate` conflated a safety suite blocking an attack with a
capability suite refusing a legitimate request. Those move a single number in
the same direction while meaning opposite things, which is worse than not
reporting it.
"""

from __future__ import annotations

import pytest
from hopsworks_agents.eval.metrics import (
    EVALUATOR_FAMILIES,
    SCORE_BUCKETS,
    run_metrics,
    variance,
)
from hopsworks_agents.eval.models import (
    EvaluatorResult,
    TraceStatus,
    Trial,
    TrialStatus,
)


def trial(task_id="a", passed=True, index=0, results=(), **kwargs) -> Trial:
    return Trial(
        trial_id=f"{task_id}-{index}",
        run_id="r",
        task_id=task_id,
        task_version=1,
        trial_index=index,
        deployment_id=7,
        status=TrialStatus.PASSED if passed else TrialStatus.FAILED,
        trace_status=TraceStatus.RECEIVED,
        latency_ms=100.0,
        evaluator_results=list(results),
        **kwargs,
    )


def result(
    kind="contains", passed=True, score=None, ungradable=False
) -> EvaluatorResult:
    return EvaluatorResult(
        evaluator_name=kind,
        evaluator_type=kind,
        score=score if score is not None else (1.0 if passed else 0.0),
        passed=passed,
        ungradable=ungradable,
    )


def rows(trials, **kwargs):
    return run_metrics("r", "s", 7, trials, **kwargs)


def at(trials, scope, name, value="", **kwargs):
    for row in rows(trials, **kwargs):
        if (
            row["metric_scope"] == scope
            and row["metric_name"] == name
            and row["metric_scope_value"] == value
        ):
            return row["metric_value"]
    return None


class TestScopes:
    def test_task_pass_rate_is_emitted_per_task(self):
        trials = [
            trial("a", True, 0),
            trial("a", True, 1),
            trial("b", True, 0),
            trial("b", False, 1),
        ]
        assert at(trials, "task", "pass_rate", "a") == 1.0
        assert at(trials, "task", "pass_rate", "b") == 0.5

    def test_task_scope_reports_reliability_separately(self):
        trials = [trial("b", True, 0), trial("b", False, 1)]
        assert at(trials, "task", "pass_all_k", "b") == 0.0
        assert at(trials, "task", "trial_variance", "b") == 0.25

    def test_category_rows_appear_only_for_tasks_that_have_one(self):
        trials = [trial("a"), trial("b", passed=False)]
        emitted = rows(trials, categories={"a": "billing"})
        categories = [r for r in emitted if r["metric_scope"] == "category"]
        assert len(categories) == 1
        assert categories[0]["metric_scope_value"] == "billing"
        assert categories[0]["metric_value"] == 1.0

    def test_per_evaluator_rows_include_how_often_it_could_not_judge(self):
        # a evaluator that never manages a verdict contributes nothing while
        # looking like coverage
        trials = [
            trial(results=[result("tool_call", ungradable=True)]),
            trial(index=1, results=[result("tool_call", passed=True)]),
        ]
        assert at(trials, "evaluator", "ungradable_rate", "tool_call") == 0.5
        assert at(trials, "evaluator", "pass_rate", "tool_call") == 1.0

    def test_evaluator_family_rates_are_kept_apart(self):
        # one pass rate hides which half broke: right answers, wrong tools
        trials = [
            trial(
                results=[
                    result("contains", passed=True),
                    result("tool_call", passed=False),
                ]
            )
        ]
        assert at(trials, "evaluator_family", "pass_rate", "final_answer") == 1.0
        assert at(trials, "evaluator_family", "pass_rate", "tool_use") == 0.0

    def test_a_evaluator_type_with_no_family_is_counted_in_none(self):
        trials = [trial(results=[result("function", passed=False)])]
        families = [r for r in rows(trials) if r["metric_scope"] == "evaluator_family"]
        assert families == []

    def test_the_score_distribution_sums_to_one(self):
        trials = [
            trial(
                results=[
                    result(score=0.1, passed=False),
                    result(score=0.5, passed=False),
                    result(score=0.95, passed=True),
                    result(score=1.0, passed=True),
                ]
            )
        ]
        buckets = {
            r["metric_scope_value"]: r["metric_value"]
            for r in rows(trials)
            if r["metric_scope"] == "score_bucket"
        }
        assert set(buckets) == set(SCORE_BUCKETS)
        assert sum(buckets.values()) == pytest.approx(1.0)
        assert buckets["0.8-1.0"] == 0.5

    def test_ungradable_results_are_left_out_of_the_distribution(self):
        trials = [trial(results=[result(score=1.0), result(ungradable=True)])]
        buckets = [r for r in rows(trials) if r["metric_scope"] == "score_bucket"]
        assert sum(r["metric_value"] for r in buckets) == pytest.approx(1.0)
        assert (
            next(r for r in buckets if r["metric_scope_value"] == "0.8-1.0")[
                "metric_value"
            ]
            == 1.0
        )


class TestSafetyAndOverRefusal:
    def blocked(self, task_id="a"):
        return Trial(
            trial_id=task_id,
            run_id="r",
            task_id=task_id,
            task_version=1,
            trial_index=0,
            deployment_id=7,
            status=TrialStatus.BLOCKED_BY_GUARDRAIL,
            trace_status=TraceStatus.RECEIVED,
        )

    def test_a_block_in_a_capability_suite_is_over_refusal(self):
        trials = [self.blocked(), trial("b")]
        assert at(trials, "run", "over_refusal_rate", blocks_are_success=False) == 0.5
        assert (
            at(trials, "run", "safety_violation_rate", blocks_are_success=False) == 0.0
        )

    def test_a_block_in_a_safety_suite_is_not_over_refusal(self):
        # it is the desired outcome; counting it as over-refusal would penalise
        # exactly the behaviour the suite exists to confirm
        trials = [self.blocked(), trial("b")]
        assert at(trials, "run", "over_refusal_rate", blocks_are_success=True) == 0.0

    def test_a_failing_trial_in_a_safety_suite_is_a_violation(self):
        trials = [trial("a", passed=False), trial("b", passed=True)]
        assert (
            at(trials, "run", "safety_violation_rate", blocks_are_success=True) == 0.5
        )

    def test_violations_are_reported_as_zero_elsewhere_not_omitted(self):
        # so a dashboard can plot the series across suites without holes
        trials = [trial("a", passed=False)]
        assert (
            at(trials, "run", "safety_violation_rate", blocks_are_success=False) == 0.0
        )

    def test_the_raw_block_rate_is_still_reported(self):
        trials = [self.blocked(), trial("b")]
        assert at(trials, "run", "guardrail_block_rate", blocks_are_success=True) == 0.5


class TestCostAndTokens:
    def test_tokens_are_summed_across_trials(self):
        trials = [
            trial("a", input_tokens=100, output_tokens=20),
            trial("b", index=1, input_tokens=50, output_tokens=5),
        ]
        assert at(trials, "run", "input_tokens") == 150.0
        assert at(trials, "run", "output_tokens") == 25.0
        assert at(trials, "run", "total_tokens") == 175.0

    def test_an_unpriced_run_reports_zero_cost_and_says_why(self):
        # 0.0 alone reads as "it was free"; the companion rate says "nothing
        # was priced"
        trials = [trial("a", input_tokens=100)]
        assert at(trials, "run", "estimated_cost") == 0.0
        assert at(trials, "run", "costed_trial_rate") == 0.0

    def test_a_priced_run_reports_both(self):
        trials = [
            trial("a", estimated_cost=0.004),
            trial("b", index=1, estimated_cost=0.002),
        ]
        assert at(trials, "run", "estimated_cost") == pytest.approx(0.006)
        assert at(trials, "run", "costed_trial_rate") == 1.0

    def test_partly_priced_runs_are_visible_as_such(self):
        trials = [trial("a", estimated_cost=0.004), trial("b", index=1)]
        assert at(trials, "run", "costed_trial_rate") == 0.5


class TestVarianceAndFlakiness:
    def test_variance_of_a_single_trial_is_zero(self):
        # one trial says nothing about how much the agent varies
        assert variance([1.0]) == 0.0

    def test_variance_rises_with_disagreement(self):
        assert variance([1.0, 0.0]) == 0.25
        assert variance([1.0, 1.0]) == 0.0

    def test_flaky_rate_accompanies_the_count(self):
        trials = [
            trial("a", True, 0),
            trial("a", False, 1),
            trial("b", True, 0),
            trial("b", True, 1),
        ]
        assert at(trials, "run", "flaky_task_count") == 1.0
        assert at(trials, "run", "flaky_task_rate") == 0.5


def test_every_spec_evaluator_type_has_a_family_or_is_deliberately_absent():
    # a new evaluator silently missing from the map would quietly stop counting
    # towards any family pass rate
    from hopsworks_agents.eval.evaluator_spec import SPEC_TYPES

    unmapped = set(SPEC_TYPES) - set(EVALUATOR_FAMILIES)
    assert unmapped == set(), f"evaluator types with no family: {sorted(unmapped)}"
