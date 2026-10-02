"""The LLM judge, which is the least trustworthy evaluator in the set.

Almost every test is about that: what happens when the judge misbehaves. A
judge that fails must never produce a score, because a score is a claim about
the agent and a broken judge has made no claim about anything.
"""

import pytest
from hopsworks_agents.eval.judges import (
    LlmJudgeEvaluator,
    _json_from,
    pairwise_verdict,
)
from hopsworks_agents.eval.models import Task, Trial


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
        "input_messages": '[{"role": "user", "content": "why is the sky blue?"}]',
        "rubric": "Mentions scattering. Does not invent a source.",
    }
    defaults.update(kwargs)
    return Task(**_expects(defaults))


def trial(answer="Rayleigh scattering."):
    return Trial(
        trial_id="x",
        run_id="r",
        task_id="t1",
        task_version=1,
        trial_index=0,
        deployment_id=7,
        final_output=answer,
    )


def judge(reply, **config_overrides):
    """A judge with no criteria named — one `overall` score, scored 1-5."""
    from hopsworks_agents.eval.judge_config import parse_judge_config

    config = parse_judge_config({"type": "llm_judge", **config_overrides})
    return LlmJudgeEvaluator(lambda _prompt: reply, config)


# The unified judge asks for per-criterion scores even when there is one
# criterion, so a single-score reply names it.
GOOD = '{"scores": {"overall": 5}, "reasoning": {"overall": "Correct."}}'


class TestGrading:
    def test_a_clean_verdict_is_used(self):
        result = judge(GOOD).grade(task(), trial(), None)
        assert result.passed is True
        assert result.score == pytest.approx(1.0)
        assert result.ungradable is False

    def test_the_judge_model_is_recorded(self):
        # a score that moved because the judge changed is not an agent
        # regression, and without the model on the row the two look identical
        result = judge(GOOD, model="claude-opus-5").grade(task(), trial(), None)
        assert result.assertions["judge_model"] == "claude-opus-5"

    def test_one_unnamed_criterion_behaves_like_any_other(self):
        # there is no separate single-score judge: a configuration naming no
        # criteria gets one called `overall` and the same code path
        result = judge(GOOD).grade(task(), trial(), None)
        assert set(result.assertions["criteria"]) == {"overall"}

    def test_the_threshold_decides_the_verdict(self):
        # the model no longer gets to return its own pass/fail: one trial
        # passing at 0.6 while another fails at 0.7 because the judge felt
        # differently makes results incomparable
        reply = '{"scores": {"overall": 3}}'
        assert judge(reply).grade(task(), trial(), None).passed is False
        assert (
            judge(reply, thresholds={"pass_score": 3})
            .grade(task(), trial(), None)
            .passed
            is True
        )

    def test_scores_are_clamped(self):
        # a model told to answer 1-5 and returning 7 has not found extra quality
        reply = '{"scores": {"overall": 7}}'
        assert judge(reply).grade(task(), trial(), None).score == 1.0


class TestWhenTheJudgeMisbehaves:
    """Every one of these must be ungradable, never a zero."""

    def test_a_judge_that_raises_is_ungradable(self):
        def explode(_prompt):
            raise RuntimeError("rate limited")

        result = LlmJudgeEvaluator(explode).grade(task(), trial(), None)
        assert result.ungradable is True
        assert result.score == 0.0
        assert "rate limited" in result.reason

    def test_unparseable_output_is_ungradable(self):
        result = judge("I think it was pretty good actually").grade(
            task(), trial(), None
        )
        assert result.ungradable is True

    def test_a_non_numeric_score_is_ungradable(self):
        reply = '{"score": "high", "passed": true, "reason": "good"}'
        assert judge(reply).grade(task(), trial(), None).ungradable is True

    def test_grading_against_nothing_is_refused(self):
        # a judge given no rubric and no expected answer still returns a number,
        # and that number is noise dressed as a measurement
        bare = task(rubric="", expected_output="")
        assert judge(GOOD).grade(bare, trial(), None).ungradable is True

    def test_an_empty_answer_is_ungradable(self):
        assert judge(GOOD).grade(task(), trial(answer=""), None).ungradable is True


class TestJsonSalvage:
    def test_plain_json(self):
        assert _json_from('{"score": 1}') == {"score": 1}

    def test_fenced_json(self):
        # models fence output constantly; discarding a valid judgement over
        # formatting would be its own kind of wrong
        assert _json_from('```json\n{"score": 1}\n```') == {"score": 1}

    def test_json_with_a_preamble(self):
        assert _json_from('Here is my grade:\n{"score": 0.5}') == {"score": 0.5}

    def test_prose_is_not_salvaged_into_something(self):
        assert _json_from("looks fine to me") is None

    def test_a_json_array_is_not_a_verdict(self):
        assert _json_from("[1, 2, 3]") is None


class TestPairwise:
    def test_a_winner_is_reported(self):
        result = pairwise_verdict(
            lambda _p: '{"winner": "b", "reason": "more complete"}',
            "q",
            "short",
            "thorough",
        )
        assert result["winner"] == "b"

    def test_a_tie_is_kept_as_a_tie(self):
        # forcing a winner would read downstream as a real difference between
        # two versions that are equivalent — the common case when comparing
        # close deployments
        result = pairwise_verdict(
            lambda _p: '{"winner": "tie", "reason": "equivalent"}', "q", "a", "b"
        )
        assert result["winner"] == "tie"

    def test_an_unusable_verdict_is_unknown_not_a_winner(self):
        result = pairwise_verdict(lambda _p: "the first one I guess", "q", "a", "b")
        assert result["winner"] == "unknown"

    def test_a_failed_call_is_unknown(self):
        def explode(_p):
            raise ConnectionError("no route")

        assert pairwise_verdict(explode, "q", "a", "b")["winner"] == "unknown"
