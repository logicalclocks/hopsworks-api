"""Aggregate trials into ``agent_eval_run_metrics`` rows.

Computed once, here, and read by everything else. A second implementation in
the UI would eventually disagree with the one the release gate uses, and the
disagreement would surface as "the dashboard says we passed but promotion is
blocked".

``pass^k`` is the metric most incumbents do not surface and the one that
matters for agents: an agent that succeeds four times in five is not 80% good
at a task, it is unreliable at it, and a user hits the fifth case.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from .models import TraceStatus, Trial, TrialStatus, gradable_trials


if TYPE_CHECKING:
    from collections.abc import Sequence


# Which part of an agent's behaviour a evaluator speaks to.
#
# The split is a judgement call and worth stating rather than leaving implied:
# **final answer** is what the agent said, **tool use** is which tools it
# invoked and how, **trajectory** is the shape of the run — the order things
# happened in, whether results fed back into the reasoning, and whether the
# world ended up as it should.
#
# `sql_state` sits in trajectory because it asserts what the run *did* rather
# than what it *said*. `human_review` sits in final answer because that is what
# a reviewer is nearly always asked about. Neither placement is forced, and a
# evaluator whose type is unknown here is counted in no family rather than
# guessed into one.
EVALUATOR_FAMILIES: dict[str, str] = {
    "exact_match": "final_answer",
    "contains": "final_answer",
    "regex": "final_answer",
    "json_schema": "final_answer",
    "llm_judge": "final_answer",
    "pairwise": "final_answer",
    "human_review": "final_answer",
    "tool_call": "tool_use",
    "no_tool_error": "tool_use",
    "tool_arguments": "tool_use",
    "tool_arguments_judge": "tool_use",
    "no_unnecessary_tools": "tool_use",
    "tool_retries": "tool_use",
    "tool_latency": "tool_use",
    "tool_order": "trajectory",
    "tool_result_used": "trajectory",
    "sql_state": "trajectory",
}

# Coarse enough to read at a glance, fine enough to show a judge clustering at
# one value — which is the most common way a rubric turns out to be useless.
SCORE_BUCKETS = ("0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0")


def _bucket(score: float) -> str:
    index = min(int(max(0.0, min(1.0, score)) * 5), 4)
    return SCORE_BUCKETS[index]


def variance(values: Sequence[float]) -> float:
    """Population variance.

    Zero for a single value, which is the honest answer:

    one trial says nothing about how much the agent varies.
    """
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return sum((v - mean) ** 2 for v in values) / len(values)


def _passed(trial: Trial) -> bool:
    return trial.status is TrialStatus.PASSED


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def pass_at_k(trials: Sequence[Trial]) -> float:
    """Fraction of tasks that succeeded at least once across their trials."""
    by_task = _by_task(trials)
    if not by_task:
        return 0.0
    return sum(
        1 for task_trials in by_task.values() if any(_passed(t) for t in task_trials)
    ) / len(by_task)


def pass_all_k(trials: Sequence[Trial]) -> float:
    """Fraction of tasks that succeeded on *every* trial — ``pass^k``.

    The reliability number. A task that passes 4 of 5 counts here as a failure,
    which is the honest reading: the agent is non-deterministic and a user will
    meet the fifth case.
    """
    by_task = _by_task(trials)
    if not by_task:
        return 0.0
    return sum(
        1 for task_trials in by_task.values() if all(_passed(t) for t in task_trials)
    ) / len(by_task)


def flaky_tasks(trials: Sequence[Trial]) -> list[str]:
    """Tasks that both passed and failed across their trials.

    Worth naming separately: a flaky task is neither a regression to fix nor a
    capability that works, and averaging it into a pass rate hides it.
    """
    flaky = []
    for task_id, task_trials in _by_task(trials).items():
        outcomes = {_passed(t) for t in task_trials}
        if len(outcomes) > 1:
            flaky.append(task_id)
    return sorted(flaky)


def _by_task(trials: Sequence[Trial]) -> dict[str, list[Trial]]:
    grouped: dict[str, list[Trial]] = defaultdict(list)
    for trial in gradable_trials(trials):
        grouped[trial.task_id].append(trial)
    return dict(grouped)


def _tool_error_rate(trials: Sequence[Trial]) -> float:
    """Share of trials with a visible trajectory in which a tool call failed."""
    seen = [
        t
        for t in trials
        if t.trace_status is not TraceStatus.MISSING and t.tool_error_count is not None
    ]
    if not seen:
        return 0.0
    return sum(1 for t in seen if t.tool_error_count) / len(seen)


def run_metrics(
    run_id: str,
    suite_id: str,
    deployment_id: int,
    trials: Sequence[Trial],
    blocks_are_success: bool = False,
    categories: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Every metric a dashboard needs, at every scope it needs them.

    Rows carry ``metric_scope`` and ``metric_scope_value`` so one table answers
    "how did the run do", "how did this task do" and "how did this evaluator
    behave" without a second aggregation somewhere else that could disagree.
    """
    gradable = gradable_trials(trials)
    latencies = [t.latency_ms for t in gradable if t.latency_ms]
    passed = sum(1 for t in gradable if _passed(t))
    is_safety = bool(blocks_are_success)

    blocked = sum(1 for t in trials if t.status is TrialStatus.BLOCKED_BY_GUARDRAIL)
    tokens_in = sum(t.input_tokens or 0 for t in trials)
    tokens_out = sum(t.output_tokens or 0 for t in trials)
    costs = [t.estimated_cost for t in trials if t.estimated_cost is not None]
    task_count = len({t.task_id for t in trials})

    values: dict[str, float] = {
        "pass_rate": passed / len(gradable) if gradable else 0.0,
        "pass_at_k": pass_at_k(trials),
        "pass_all_k": pass_all_k(trials),
        "p50_latency_ms": percentile(latencies, 50),
        "p95_latency_ms": percentile(latencies, 95),
        "p99_latency_ms": percentile(latencies, 99),
        # Not a quality metric: a high rate means the observability pipeline is
        # broken, and trajectory numbers from this run cannot be trusted.
        "trace_missing_rate": (
            sum(1 for t in trials if t.trace_status is TraceStatus.MISSING)
            / len(trials)
            if trials
            else 0.0
        ),
        # Raw, uninterpreted. The two below say what it *means*, which depends
        # entirely on what the suite was testing.
        "guardrail_block_rate": blocked / len(trials) if trials else 0.0,
        # A safety suite is attacks by construction, so a failing trial is an
        # attack that landed. Reported as zero elsewhere rather than omitted, so
        # a dashboard can plot it across suites without holes.
        "safety_violation_rate": (
            (len(gradable) - passed) / len(gradable) if is_safety and gradable else 0.0
        ),
        # And the mirror image: a guardrail firing in a suite of legitimate
        # requests is over-refusal. Conflating the two -- which one block rate
        # does -- means a suite that got safer and one that got more timid move
        # the number identically.
        "over_refusal_rate": (
            blocked / len(trials) if not is_safety and trials else 0.0
        ),
        "flaky_task_count": float(len(flaky_tasks(trials))),
        "flaky_task_rate": len(flaky_tasks(trials)) / task_count if task_count else 0.0,
        "tool_error_rate": _tool_error_rate(trials),
        "input_tokens": float(tokens_in),
        "output_tokens": float(tokens_out),
        "total_tokens": float(tokens_in + tokens_out),
        # Summed only over trials that reported one. A run where pricing is not
        # configured reports 0.0 and `costed_trial_rate` says why.
        "estimated_cost": float(sum(costs)),
        "costed_trial_rate": len(costs) / len(trials) if trials else 0.0,
        # How much the agent varies within a task, averaged over tasks. High
        # with a decent pass rate is the signature of a flaky agent rather than
        # a wrong one.
        "trial_variance": _mean_task_variance(trials),
    }

    rows = [
        _row(
            run_id,
            suite_id,
            deployment_id,
            "run",
            "",
            name,
            value,
            task_count,
            len(trials),
        )
        for name, value in values.items()
    ]
    rows.extend(_task_rows(run_id, suite_id, deployment_id, trials))
    rows.extend(
        _category_rows(run_id, suite_id, deployment_id, trials, categories or {})
    )
    rows.extend(_evaluator_rows(run_id, suite_id, deployment_id, trials))
    rows.extend(_family_rows(run_id, suite_id, deployment_id, trials))
    rows.extend(_bucket_rows(run_id, suite_id, deployment_id, trials))
    return rows


def _row(
    run_id: str,
    suite_id: str,
    deployment_id: int,
    scope: str,
    scope_value: str,
    name: str,
    value: float,
    task_count: int,
    trial_count: int,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "suite_id": suite_id,
        "deployment_id": deployment_id,
        "metric_scope": scope,
        # empty for run scope; without the value in the key two categories
        # reporting the same metric would collide
        "metric_scope_value": scope_value,
        "metric_name": name,
        "metric_value": float(value),
        "task_count": task_count,
        "trial_count": trial_count,
    }


def _mean_task_variance(trials: Sequence[Trial]) -> float:
    per_task = [
        variance([1.0 if _passed(t) else 0.0 for t in group])
        for group in _by_task(gradable_trials(trials)).values()
    ]
    return sum(per_task) / len(per_task) if per_task else 0.0


def _task_rows(
    run_id: str, suite_id: str, deployment_id: int, trials: Sequence[Trial]
) -> list[dict[str, Any]]:
    """Per task, so a dashboard can rank what is failing without re-deriving it."""
    rows = []
    for task_id, group in _by_task(trials).items():
        gradable = gradable_trials(group)
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "task",
                task_id,
                "pass_rate",
                sum(1 for t in gradable if _passed(t)) / len(gradable)
                if gradable
                else 0.0,
                1,
                len(group),
            )
        )
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "task",
                task_id,
                "pass_all_k",
                pass_all_k(group),
                1,
                len(group),
            )
        )
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "task",
                task_id,
                "trial_variance",
                variance([1.0 if _passed(t) else 0.0 for t in gradable]),
                1,
                len(group),
            )
        )
    return rows


def _category_rows(
    run_id: str,
    suite_id: str,
    deployment_id: int,
    trials: Sequence[Trial],
    categories: dict[str, str],
) -> list[dict[str, Any]]:
    """Per task category, for suites large enough that per-task is unreadable."""
    grouped: dict[str, list[Trial]] = defaultdict(list)
    for trial in trials:
        category = categories.get(trial.task_id, "")
        if category:
            grouped[category].append(trial)
    rows = []
    for category, group in grouped.items():
        gradable = gradable_trials(group)
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "category",
                category,
                "pass_rate",
                sum(1 for t in gradable if _passed(t)) / len(gradable)
                if gradable
                else 0.0,
                len({t.task_id for t in group}),
                len(group),
            )
        )
    return rows


def _evaluator_results(trials: Sequence[Trial]):
    for trial in trials:
        yield from trial.evaluator_results


def _evaluator_rows(
    run_id: str, suite_id: str, deployment_id: int, trials: Sequence[Trial]
) -> list[dict[str, Any]]:
    """Per evaluator, including how often it could not judge.

    `ungradable_rate` is the one to watch: a evaluator that never manages a verdict
    is contributing nothing while looking like coverage.
    """
    grouped: dict[str, list[Any]] = defaultdict(list)
    for result in _evaluator_results(trials):
        grouped[result.evaluator_name].append(result)

    rows = []
    for name, results in grouped.items():
        gradable = [r for r in results if not r.ungradable]
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "evaluator",
                name,
                "pass_rate",
                sum(1 for r in gradable if r.passed) / len(gradable)
                if gradable
                else 0.0,
                0,
                len(results),
            )
        )
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "evaluator",
                name,
                "mean_score",
                sum(r.score for r in gradable) / len(gradable) if gradable else 0.0,
                0,
                len(results),
            )
        )
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "evaluator",
                name,
                "ungradable_rate",
                (len(results) - len(gradable)) / len(results) if results else 0.0,
                0,
                len(results),
            )
        )
    return rows


def _family_rows(
    run_id: str, suite_id: str, deployment_id: int, trials: Sequence[Trial]
) -> list[dict[str, Any]]:
    """Final-answer, tool-use and trajectory pass rates, kept apart.

    One pass rate hides which half of the agent broke: a release that answers
    correctly by luck while calling the wrong tools reads as healthy until the
    data changes underneath it.
    """
    grouped: dict[str, list[Any]] = defaultdict(list)
    for result in _evaluator_results(trials):
        family = EVALUATOR_FAMILIES.get(result.evaluator_type)
        if family:
            grouped[family].append(result)

    rows = []
    for family, results in grouped.items():
        gradable = [r for r in results if not r.ungradable]
        rows.append(
            _row(
                run_id,
                suite_id,
                deployment_id,
                "evaluator_family",
                family,
                "pass_rate",
                sum(1 for r in gradable if r.passed) / len(gradable)
                if gradable
                else 0.0,
                0,
                len(results),
            )
        )
    return rows


def _bucket_rows(
    run_id: str, suite_id: str, deployment_id: int, trials: Sequence[Trial]
) -> list[dict[str, Any]]:
    """The distribution of evaluator scores, as a share per bucket.

    Worth plotting because the shape is the tell: scores piling up at one value
    usually means a rubric everything satisfies, which reads as a healthy pass
    rate and measures nothing.
    """
    gradable = [r for r in _evaluator_results(trials) if not r.ungradable]
    if not gradable:
        return []
    counts = dict.fromkeys(SCORE_BUCKETS, 0)
    for result in gradable:
        counts[_bucket(result.score)] += 1
    return [
        _row(
            run_id,
            suite_id,
            deployment_id,
            "score_bucket",
            bucket,
            "share",
            count / len(gradable),
            0,
            len(gradable),
        )
        for bucket, count in counts.items()
    ]
