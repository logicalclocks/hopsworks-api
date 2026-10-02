"""LLM judges: grading what a deterministic check cannot express.

A rubric judge earns its place where correctness is not a string comparison —
"did it answer the question without inventing a policy", "was the tone right for
a refund refusal". It is also the least trustworthy evaluator in the set, so
everything here is built around that:

- The judge model is recorded on every result. A score that moved because the
  judge changed is not a regression in the agent, and without the model on the
  row the two are indistinguishable.
- A judge that fails, times out, or returns something unparseable comes back
  **ungradable**, never zero. Zero is a claim about the agent; an unparseable
  response is a claim about the judge.
- The rubric and the answer go in; the task's own expected output goes in only
  when it exists. A judge asked to score against nothing will still return a
  number, and that number means nothing.

Provider is configured per project, with the key from project secrets, so this
never carries credentials of its own.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any

from .evaluators import Trace, _ungradable
from .judge_config import (
    FAILURE_CATEGORIES,
    JudgeConfig,
    render_prompt,
    tool_calls_text,
)
from .models import EvaluatorResult, Task, Trial


if TYPE_CHECKING:
    from collections.abc import Callable


log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-sonnet-5"

PAIRWISE_PROMPT = """You are comparing two AI agent responses to the same question.

Judge which better satisfies the criteria. If they are equivalent, say "tie" — \
do not break a genuine tie arbitrarily, since a forced winner reads as a real \
difference downstream.

<question>
{question}
</question>

<response_a>
{answer_a}
</response_a>

<response_b>
{answer_b}
</response_b>
{rubric_block}
Reply with JSON only, no prose:
{{"winner": "<a|b|tie>", "reason": "<one sentence>"}}"""


def _json_from(text: str) -> dict[str, Any] | None:
    """The JSON a model returned, whatever it wrapped it in.

    Models fence JSON, prefix it with "Here is", or both. Salvaging is worth it
    because the alternative is discarding a valid judgement over formatting.
    """
    if not text:
        return None
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    candidate = fenced.group(1) if fenced else text
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except (ValueError, TypeError):
        pass
    braces = re.search(r"\{.*\}", candidate, re.S)
    if not braces:
        return None
    try:
        parsed = json.loads(braces.group(0))
        return parsed if isinstance(parsed, dict) else None
    except (ValueError, TypeError):
        return None


def pairwise_verdict(
    complete: Callable[[str], str],
    question: str,
    answer_a: str,
    answer_b: str,
    rubric: str = "",
) -> dict[str, Any]:
    """Which of two answers is better, or a tie.

    Used to compare deployments on the same task. Ties are reported rather than
    broken: a forced winner reads downstream as a real difference, and two
    equivalent answers are the common case when comparing close versions.
    """
    prompt = PAIRWISE_PROMPT.format(
        question=question,
        answer_a=answer_a,
        answer_b=answer_b,
        rubric_block=f"\n<criteria>\n{rubric}\n</criteria>\n" if rubric else "",
    )
    try:
        parsed = _json_from(complete(prompt))
    except Exception as err:  # noqa: BLE001
        return {"winner": "unknown", "reason": f"judge call failed: {err}"}
    if not parsed or parsed.get("winner") not in ("a", "b", "tie"):
        return {"winner": "unknown", "reason": "judge did not return a usable verdict"}
    return {"winner": parsed["winner"], "reason": str(parsed.get("reason", ""))[:1000]}


def anthropic_completer(
    api_key: str, model: str = DEFAULT_MODEL, max_tokens: int = 1024
) -> Callable[[str], str]:
    """A ``complete`` backed by Anthropic.

    Imported lazily so the eval package does not require a provider SDK to be
    installed for the evaluators that need no model at all.
    """

    def complete(prompt: str) -> str:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(
            block.text
            for block in response.content
            if getattr(block, "type", "") == "text"
        )

    return complete


class PairwiseEvaluator:
    """Is this answer at least as good as a reference one?

    A rubric judge scores an answer against a description. This compares it
    against a concrete answer someone already accepted, which is the sharper
    question when a suite exists to catch regressions: "no worse than what we
    shipped" is easier to agree on than "scores 0.7".

    Ties pass. A tie means the judge could not tell them apart, and failing a
    trial on that would make the evaluator a coin toss on every equivalent answer.

    Position bias is real and worth knowing about: judges favour whichever
    response they see first. The reference is presented as A and the candidate
    as B consistently, so the bias is a constant across trials rather than
    noise, and a suite compares like with like.
    """

    type = "pairwise"
    needs_trace = False

    def __init__(
        self,
        complete: Callable[[str], str],
        *,
        reference: str = "",
        name: str = "pairwise",
        model: str = DEFAULT_MODEL,
    ):
        self._complete = complete
        self.reference = reference
        self.name = name
        self.model = model

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        reference = self.reference or task.expects_text(self.name)
        if not reference:
            return _ungradable(
                self.name,
                self.type,
                "no reference answer to compare against",
            )
        if not trial.final_output:
            return _ungradable(self.name, self.type, "the agent produced no answer")

        outcome = pairwise_verdict(
            self._complete,
            question=task.asked,
            answer_a=reference,
            answer_b=trial.final_output,
            rubric=task.expects_text(self.name),
        )
        winner = outcome.get("winner")
        if winner not in ("a", "b", "tie"):
            return _ungradable(
                self.name, self.type, outcome.get("reason", "no verdict")
            )

        # b is the candidate; a tie passes, since the judge is saying it cannot
        # separate them rather than that the candidate is worse.
        passed = winner in ("b", "tie")
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=1.0 if winner == "b" else 0.5 if winner == "tie" else 0.0,
            passed=passed,
            reason=outcome.get("reason", ""),
            assertions={"winner": winner, "judge_model": self.model},
        )


TOOL_ARGUMENTS_PROMPT = """You are checking whether an AI agent called a tool \
with sensible arguments.

The arguments are syntactically valid; that has already been checked. Judge only \
whether they are the right arguments for what was asked — right entity, right \
filters, nothing invented that the question did not contain.

<question>
{question}
</question>

<tool_calls>
{calls}
</tool_calls>
{rubric_block}
Reply with JSON only, no prose:
{{"passed": <true|false>, "score": <0.0-1.0>, "reason": "<one sentence>"}}"""

TOOL_RESULT_PROMPT = """You are checking whether an AI agent used what its tools \
returned.

The question is whether the final answer reflects the tool results: values taken \
from them rather than invented, and a result that contradicts the answer treated \
as a problem. An answer that ignores a tool result and happens to be right is \
still a failure of this check.

<question>
{question}
</question>

<tool_results>
{results}
</tool_results>

<final_answer>
{answer}
</final_answer>

Reply with JSON only, no prose:
{{"passed": <true|false>, "score": <0.0-1.0>, "reason": "<one sentence>"}}"""


def _truncate(value: str, limit: int = 2000) -> str:
    return value if len(value) <= limit else value[:limit] + "…[truncated]"


def _tool_calls(trace) -> list:
    if not trace:
        return []
    calls = trace.get("tool_calls")
    return calls if isinstance(calls, list) else []


def _judged(name: str, kind: str, model: str, raw: str) -> EvaluatorResult:
    """A judge reply turned into a result, or ungradable if it is not usable."""
    parsed = _json_from(raw)
    if parsed is None or "passed" not in parsed:
        return _ungradable(
            name,
            kind,
            "judge did not return usable JSON; scoring zero here would blame the "
            "agent for the judge",
        )
    try:
        score = max(
            0.0, min(1.0, float(parsed.get("score", 1.0 if parsed["passed"] else 0.0)))
        )
    except (TypeError, ValueError):
        score = 1.0 if parsed["passed"] else 0.0
    return EvaluatorResult(
        evaluator_name=name,
        evaluator_type=kind,
        score=score,
        passed=bool(parsed["passed"]),
        reason=str(parsed.get("reason", ""))[:1000],
        assertions={"judge_model": model},
    )


class ToolArgumentsJudge:
    """Were the arguments *right*, not merely well-formed?

    ToolArgumentEvaluator answers whether the payload parses and carries the keys
    it must. This answers whether `{"order_id": "4471"}` was the right order to
    look up given what the user asked — which no schema can express.
    """

    type = "tool_arguments_judge"
    needs_trace = True

    def __init__(
        self,
        complete: Callable[[str], str],
        *,
        tool: str = "",
        name: str = "tool_arguments_judge",
        model: str = DEFAULT_MODEL,
    ):
        self._complete = complete
        self.tool = tool
        self.name = name
        self.model = model

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        calls = [
            c
            for c in _tool_calls(trace)
            if (not self.tool or c.get("name") == self.tool) and c.get("arguments")
        ]
        if not calls:
            return _ungradable(
                self.name,
                self.type,
                "no tool call recorded its arguments; nothing to judge",
            )
        rendered = "\n".join(
            f"- {c.get('name')}({_truncate(c.get('arguments', ''), 600)})"
            for c in calls
        )
        prompt = TOOL_ARGUMENTS_PROMPT.format(
            question=task.asked,
            calls=rendered,
            rubric_block=(
                f"\n<criteria>\n{rubric}\n</criteria>\n"
                if (rubric := task.expects_text(self.name))
                else ""
            ),
        )
        try:
            raw = self._complete(prompt)
        except Exception as err:  # noqa: BLE001 — a broken judge is not a broken agent
            log.warning("tool argument judge failed: %s", err)
            return _ungradable(self.name, self.type, f"judge call failed: {err}")
        return _judged(self.name, self.type, self.model, raw)


class ToolResultUsedJudge:
    """Did the answer actually use what the tools returned?

    The failure this catches is an agent that calls the right tool, ignores what
    comes back, and answers from the model's own prior — which every other
    evaluator here scores as a pass when the prior happens to be right, and which
    is exactly the behaviour that breaks when the underlying data changes.
    """

    type = "tool_result_used"
    needs_trace = True

    def __init__(
        self,
        complete: Callable[[str], str],
        *,
        name: str = "tool_result_used",
        model: str = DEFAULT_MODEL,
    ):
        self._complete = complete
        self.name = name
        self.model = model

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        results = [c for c in _tool_calls(trace) if c.get("result")]
        if not results:
            return _ungradable(
                self.name,
                self.type,
                "no tool call recorded a result; nothing to have used",
            )
        if not trial.final_output:
            return _ungradable(self.name, self.type, "the agent produced no answer")

        rendered = "\n".join(
            f"- {c.get('name')} → {_truncate(c.get('result', ''), 800)}"
            for c in results
        )
        prompt = TOOL_RESULT_PROMPT.format(
            question=task.asked, results=rendered, answer=trial.final_output
        )
        try:
            raw = self._complete(prompt)
        except Exception as err:  # noqa: BLE001
            log.warning("tool result judge failed: %s", err)
            return _ungradable(self.name, self.type, f"judge call failed: {err}")
        return _judged(self.name, self.type, self.model, raw)


class LlmJudgeEvaluator:
    """Score a trial with a model, against one criterion or several.

    There is no separate single-criterion judge. A configuration that names no
    criteria gets one called `overall`, and from there the prompt, the
    weighting, the floors and the breakdown are identical — one code path
    rather than two that drift apart.

    One model call however many criteria: seven of them over fifty tasks and
    three trials is a thousand calls rather than a hundred and fifty, and
    weights and floors are not expressible by the suite's pass policy anyway.

    The verdict is always score-then-threshold. An earlier version let the model
    return its own pass/fail, which sounds accommodating and makes results
    incomparable: one trial passes at 0.6 and another fails at 0.7 because the
    judge felt differently that time. A rubric steers the score instead.
    """

    type = "llm_judge"

    def __init__(
        self,
        complete: Callable[[str], str],
        config: JudgeConfig | None = None,
        *,
        name: str = "llm_judge",
    ):
        self._complete = complete
        self.config = config or JudgeConfig()
        self.name = name
        self.model = self.config.model or DEFAULT_MODEL
        # Only when the configuration asks to see the trajectory: a judge that
        # is not shown tool calls must not be marked ungradable for want of a
        # trace it would never have read.
        self.needs_trace = bool(
            {"tool_calls", "tool_results"} & set(self.config.inputs)
        )

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        criteria = self.config.effective_criteria()
        if not self.config.multi and not task.expects_text(self.name):
            # Nothing to grade against: a judge asked to score against nothing
            # returns a number anyway, and that number is noise dressed as a
            # measurement.
            return _ungradable(
                self.name,
                self.type,
                "no criteria, no rubric and no expected output: there is "
                "nothing to grade against",
            )
        if not trial.final_output:
            return _ungradable(self.name, self.type, "the agent produced no answer")
        if self.needs_trace and trace is None:
            return _ungradable(
                self.name,
                self.type,
                "no trace: this judge is configured to read the tool calls",
            )
        calls, results = tool_calls_text(trace)
        prompt = render_prompt(
            self.config,
            question=task.asked,
            answer=trial.final_output,
            expected=task.expects_text(self.name),
            rubric=task.expects_text(self.name),
            tool_calls=calls,
            tool_results=results,
            transcript=trial.transcript,
        )

        try:
            raw = self._complete(prompt)
        except Exception as err:  # noqa: BLE001 — a broken judge is not a broken agent
            log.warning("judge call failed: %s", err)
            return _ungradable(self.name, self.type, f"judge call failed: {err}")

        parsed = _json_from(raw)
        scores = (parsed or {}).get("scores")
        if not isinstance(scores, dict):
            return _ungradable(
                self.name,
                self.type,
                "judge did not return per-criterion scores; scoring zero here "
                "would blame the agent for the judge",
            )

        graded: dict[str, float] = {}
        missing: list[str] = []
        for criterion in criteria:
            value = scores.get(criterion.name)
            try:
                graded[criterion.name] = float(value)
            except (TypeError, ValueError):
                missing.append(criterion.name)

        if not graded:
            return _ungradable(
                self.name, self.type, "judge scored none of the criteria"
            )
        if missing:
            # Partial results are worse than none: a weighted total over the
            # criteria that happened to come back is a different measurement
            # each time, and comparing it across runs is meaningless.
            return _ungradable(
                self.name,
                self.type,
                f"judge did not score {', '.join(missing)}",
            )

        weights = {c.name: c.weight for c in criteria}
        total_weight = sum(weights.values())
        weighted = sum(graded[n] * weights[n] for n in graded) / total_weight

        # A floor breached fails the trial whatever the weighted total says.
        # Seven good scores hiding one catastrophic safety result is exactly
        # what anyone configuring a critical dimension is guarding against.
        breached = [
            c.name
            for c in criteria
            if c.critical_min is not None and graded[c.name] < c.critical_min
        ]
        passed = not breached and weighted >= self.config.pass_score

        reasons = (parsed or {}).get("reasoning") or {}
        category = str((parsed or {}).get("failure_category") or "")
        if category and category not in FAILURE_CATEGORIES:
            category = "other"

        if breached:
            reason = "below the floor on " + ", ".join(breached)
        elif passed:
            reason = ""
        else:
            reason = (
                f"scored {weighted:.2f} of {self.config.score_max:g}, "
                f"needs {self.config.pass_score:g}"
            )
        detail = (
            "; ".join(f"{name}: {text}" for name, text in reasons.items() if text)
            if isinstance(reasons, dict)
            else ""
        )

        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=self.config.normalise(weighted),
            passed=passed,
            reason=(reason + (f" ({detail})" if detail and not passed else ""))[:1000],
            assertions={
                # Raw, in the scale the judge was asked for, so a person reading
                # this sees the numbers the prompt talked about.
                "criteria": graded,
                "criteria_normalised": {
                    n: self.config.normalise(v) for n, v in graded.items()
                },
                "weighted_score": weighted,
                "pass_score": self.config.pass_score,
                "critical_breached": breached,
                "failure_category": category,
                "reasoning": reasons if isinstance(reasons, dict) else {},
                "judge_model": self.model,
            },
        )
