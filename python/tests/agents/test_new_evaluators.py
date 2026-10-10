"""The evaluators that close the design doc's list, and the pass policy.

Two themes, both about not answering a question nobody answered: a state check
that could not run is ungradable rather than passing, and a task that asked for
a person is held open rather than settled by the other evaluators agreeing.
"""

from __future__ import annotations

import json

import pytest
from hopsworks_agents.eval.evaluators import (
    HumanReviewEvaluator,
    SqlStateEvaluator,
    awaits_review,
    verdict,
)
from hopsworks_agents.eval.judges import PairwiseEvaluator
from hopsworks_agents.eval.models import EvaluatorResult, PassPolicy, Task, Trial


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
        input_messages=json.dumps([{"role": "user", "content": "cancel 4471"}]),
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


class TestSqlStateEvaluator:
    def test_state_matching_the_expectation_passes(self):
        evaluator = SqlStateEvaluator(
            "SELECT status FROM orders WHERE id = '4471'",
            expect="cancelled",
            query=lambda _sql: "cancelled",
        )
        result = evaluator.grade(task(), trial(), None)
        assert result.passed and result.score == 1.0

    def test_an_agent_that_only_claims_to_have_acted_fails(self):
        # the whole point: the answer says "cancelled", the table says otherwise
        evaluator = SqlStateEvaluator(
            "SELECT status FROM orders", expect="cancelled", query=lambda _s: "open"
        )
        result = evaluator.grade(task(), trial("I've cancelled order 4471."), None)
        assert result.passed is False
        assert "expected 'cancelled', found 'open'" in result.reason

    def test_no_query_function_is_ungradable_not_a_pass(self):
        # "I could not check" must never read as "the state was right"
        result = SqlStateEvaluator("SELECT 1", expect=1).grade(task(), trial(), None)
        assert result.ungradable and result.passed is False

    def test_a_failing_query_is_ungradable_not_a_failure(self):
        def boom(_sql: str):
            raise RuntimeError("connection refused")

        result = SqlStateEvaluator("SELECT 1", expect=1, query=boom).grade(
            task(), trial(), None
        )
        assert result.ungradable
        assert "connection refused" in result.reason

    @pytest.mark.parametrize(
        "returned",
        [
            "cancelled",
            ["cancelled"],
            [["cancelled"]],
            [("cancelled", "other")],
        ],
    )
    def test_reads_the_first_cell_whatever_shape_the_session_returns(self, returned):
        evaluator = SqlStateEvaluator(
            "SELECT 1", expect="cancelled", query=lambda _s: returned
        )
        assert evaluator.grade(task(), trial(), None).passed

    def test_an_empty_result_is_not_a_match(self):
        evaluator = SqlStateEvaluator(
            "SELECT 1", expect="cancelled", query=lambda _s: []
        )
        assert evaluator.grade(task(), trial(), None).passed is False

    def test_numbers_compare_across_types(self):
        # a driver returning 30 and a task expecting "30" agree
        evaluator = SqlStateEvaluator("SELECT 1", expect="30", query=lambda _s: 30)
        assert evaluator.grade(task(), trial(), None).passed


class TestHumanReviewEvaluator:
    def test_it_defers_rather_than_judging(self):
        result = HumanReviewEvaluator().grade(task(), trial(), None)
        assert result.ungradable
        assert result.assertions["awaiting_review"] is True

    def test_the_prompt_reaches_the_reviewer(self):
        result = HumanReviewEvaluator("Is the tone right for a refund refusal?").grade(
            task(), trial(), None
        )
        assert "tone" in result.reason

    def test_awaits_review_spots_it_among_other_results(self):
        passing = EvaluatorResult("contains", "contains", 1.0, True)
        pending = HumanReviewEvaluator().grade(task(), trial(), None)
        assert awaits_review([passing, pending]) is True
        assert awaits_review([passing]) is False


class TestPairwiseEvaluator:
    def judge(self, winner: str):
        return lambda _prompt: json.dumps({"winner": winner, "reason": "because"})

    def test_the_candidate_winning_passes(self):
        evaluator = PairwiseEvaluator(self.judge("b"), reference="the old answer")
        result = evaluator.grade(task(), trial(), None)
        assert result.passed and result.score == 1.0
        assert result.assertions["winner"] == "b"

    def test_a_tie_passes(self):
        # a tie is the judge saying it cannot separate them; failing on that
        # makes the evaluator a coin toss on every equivalent answer
        evaluator = PairwiseEvaluator(self.judge("tie"), reference="the old answer")
        result = evaluator.grade(task(), trial(), None)
        assert result.passed and result.score == 0.5

    def test_the_reference_winning_fails(self):
        evaluator = PairwiseEvaluator(self.judge("a"), reference="the old answer")
        assert evaluator.grade(task(), trial(), None).passed is False

    def test_it_falls_back_to_the_expected_output_as_the_reference(self):
        evaluator = PairwiseEvaluator(self.judge("b"))
        assert evaluator.grade(
            task(expected_output="the good one"), trial(), None
        ).passed

    def test_no_reference_is_ungradable(self):
        result = PairwiseEvaluator(self.judge("b")).grade(task(), trial(), None)
        assert result.ungradable
        assert "no reference" in result.reason

    def test_no_answer_is_ungradable(self):
        evaluator = PairwiseEvaluator(self.judge("b"), reference="ref")
        result = evaluator.grade(task(), trial(output=""), None)
        assert result.ungradable

    def test_an_unusable_verdict_is_ungradable_not_a_failure(self):
        # blaming the agent for a judge that returned prose is the error this
        # whole family of evaluators is most prone to
        evaluator = PairwiseEvaluator(lambda _p: "I think B is nicer", reference="ref")
        result = evaluator.grade(task(), trial(), None)
        assert result.ungradable and result.passed is False

    def test_the_judge_model_is_recorded(self):
        evaluator = PairwiseEvaluator(
            self.judge("b"), reference="ref", model="some-model"
        )
        assert evaluator.grade(task(), trial(), None).assertions["judge_model"] == (
            "some-model"
        )


class TestPassPolicy:
    def results(self, *passed: bool) -> list[EvaluatorResult]:
        return [
            EvaluatorResult(f"g{i}", "contains", 1.0 if p else 0.0, p)
            for i, p in enumerate(passed)
        ]

    def test_all_is_the_default_and_needs_every_evaluator(self):
        assert verdict(self.results(True, True)) is True
        assert verdict(self.results(True, False)) is False

    def test_any_passes_on_one(self):
        assert verdict(self.results(True, False), PassPolicy.ANY) is True
        assert verdict(self.results(False, False), PassPolicy.ANY) is False

    def test_threshold_reads_the_mean_score(self):
        scored = [
            EvaluatorResult("a", "llm_judge", 0.8, True),
            EvaluatorResult("b", "llm_judge", 0.4, False),
        ]
        assert verdict(scored, PassPolicy.THRESHOLD, threshold=0.5) is True
        assert verdict(scored, PassPolicy.THRESHOLD, threshold=0.7) is False

    def test_a_policy_given_as_a_string_works(self):
        # it arrives from the REST payload as one
        assert verdict(self.results(True, False), "any") is True

    def test_nothing_gradable_is_none_under_every_policy(self):
        ungradable = [EvaluatorResult("a", "x", 0.0, False, ungradable=True)]
        for policy in ("all", "any", "threshold"):
            assert verdict(ungradable, policy) is None

    def test_ungradable_results_are_excluded_rather_than_counted(self):
        mixed = [
            EvaluatorResult("a", "contains", 1.0, True),
            EvaluatorResult("b", "tool_call", 0.0, False, ungradable=True),
        ]
        assert verdict(mixed) is True
        assert verdict(mixed, PassPolicy.THRESHOLD, threshold=0.9) is True

    def test_an_unknown_policy_falls_back_to_all(self):
        # a stored value from a newer version must not silently loosen the bar
        assert verdict(self.results(True, False), "whatever-is-next") is False
