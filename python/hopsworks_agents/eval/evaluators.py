"""Evaluators: given a task, a trial and (maybe) a trace, produce a score.

Two rules run through all of them.

**A evaluator never raises into the runner.** One badly written custom evaluator must
not fail a whole run, so :func:`run_evaluators` catches and converts to a
``EVALUATOR_ERROR`` result. That failure is the harness's, not the agent's, and is
excluded from pass rates.

**No trace is not a failing score.** Spans reach the feature store
asynchronously and sometimes never arrive at all — the sidecar logs and drops
on insert failure. A trajectory evaluator handed ``trace=None`` must return
``ungradable``, because scoring it zero would report a broken observability
pipeline as a broken agent, and that is the kind of wrong answer that gets a
good deployment blocked.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, Protocol

from .models import EvaluatorResult, Task, Trial


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from .models import PassPolicy


# A trace as the runner assembles it: the `agent_trace_features` row plus the
# raw spans, so trajectory evaluators can inspect tool order.
Trace = dict[str, Any]


class Evaluator(Protocol):
    name: str
    type: str
    # True when this evaluator reads the trajectory rather than only the answer,
    # so the runner knows which evaluators to mark ungradable without a trace.
    needs_trace: bool

    def grade(
        self, task: Task, trial: Trial, trace: Trace | None
    ) -> EvaluatorResult: ...


def _ungradable(name: str, kind: str, reason: str) -> EvaluatorResult:
    return EvaluatorResult(
        evaluator_name=name,
        evaluator_type=kind,
        score=0.0,
        passed=False,
        reason=reason,
        ungradable=True,
    )


class ExactMatchEvaluator:
    """Final answer equals the expected output, ignoring surrounding space."""

    type = "exact_match"
    needs_trace = False

    def __init__(self, name: str = "exact_match", case_sensitive: bool = False):
        self.name = name
        self.case_sensitive = case_sensitive

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        expected, actual = task.expects_text(self.name), trial.final_output.strip()
        if not self.case_sensitive:
            expected, actual = expected.lower(), actual.lower()
        passed = expected == actual
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if passed else 0.0,
            passed,
            "exact match" if passed else f"expected {expected!r}",
        )


class ContainsEvaluator:
    """The answer mentions the expected string.

    Weaker than exact match and much more useful for free-text answers, where
    an exact match asserts the model's phrasing rather than its correctness.
    """

    type = "contains"
    needs_trace = False

    def __init__(self, name: str = "contains", expected: str | None = None):
        self.name = name
        self.expected = expected

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        needle = (
            self.expected.strip()
            if self.expected is not None
            else task.expects_text(self.name)
        )
        passed = needle.lower() in trial.final_output.lower()
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if passed else 0.0,
            passed,
            "found" if passed else f"{needle!r} not in the answer",
        )


class RegexEvaluator:
    type = "regex"
    needs_trace = False

    def __init__(self, pattern: str, name: str = "regex", should_match: bool = True):
        self.name = name
        self.pattern = re.compile(pattern)
        self.should_match = should_match

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        matched = bool(self.pattern.search(trial.final_output))
        passed = matched is self.should_match
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if passed else 0.0,
            passed,
            f"pattern {'matched' if matched else 'did not match'}",
            {"matched": matched},
        )


class JsonSchemaEvaluator:
    """The answer parses as JSON and carries the required keys.

    Deliberately shallow: full JSON Schema would be a dependency, and the
    common failure this catches is an agent that stopped emitting structured
    output at all.
    """

    type = "json_schema"
    needs_trace = False

    def __init__(self, required_keys: Sequence[str], name: str = "json_schema"):
        self.name = name
        self.required_keys = list(required_keys)

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        try:
            parsed = json.loads(trial.final_output)
        except (ValueError, TypeError):
            return EvaluatorResult(
                self.name,
                self.type,
                0.0,
                False,
                "answer is not valid JSON",
                {"parsed": False},
            )
        if not isinstance(parsed, dict):
            return EvaluatorResult(
                self.name,
                self.type,
                0.0,
                False,
                "answer is not a JSON object",
                {"parsed": True},
            )
        missing = [k for k in self.required_keys if k not in parsed]
        return EvaluatorResult(
            self.name,
            self.type,
            0.0 if missing else 1.0,
            not missing,
            f"missing keys: {missing}" if missing else "all required keys present",
            {"missing": missing},
        )


class ToolCallEvaluator:
    """Required tools were called and forbidden ones were not.

    Reads the trajectory, so it is ungradable without a trace.
    """

    type = "tool_call"
    needs_trace = True

    def __init__(self, name: str = "tool_call"):
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        if trace is None:
            return _ungradable(
                self.name,
                self.type,
                "no trace: tool use cannot be judged from the answer alone",
            )
        called = _tool_names(trace)
        required, not_allowed = task.expects_tools(self.name)
        missing = [t for t in required if t not in called]
        forbidden = [t for t in not_allowed if t in called]
        passed = not missing and not forbidden
        reasons = []
        if missing:
            reasons.append(f"never called {missing}")
        if forbidden:
            reasons.append(f"called forbidden {forbidden}")
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if passed else 0.0,
            passed,
            "; ".join(reasons) or "tool use as expected",
            {
                "called": called,
                "missing_required": missing,
                "called_forbidden": forbidden,
            },
        )


class ToolOrderEvaluator:
    """Required tools were called in the order the task lists them.

    Order matters for agents whose steps depend on each other — looking up a
    customer before issuing their refund. Subsequence, not equality: extra
    calls in between are allowed, since an agent that also checked something
    else has not done the wrong thing.
    """

    type = "tool_order"
    needs_trace = True

    def __init__(self, name: str = "tool_order"):
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        if trace is None:
            return _ungradable(self.name, self.type, "no trace: order cannot be judged")
        called = _tool_names(trace)
        remaining = task.expects_list(self.name)
        for name in called:
            if remaining and name == remaining[0]:
                remaining.pop(0)
        passed = not remaining
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if passed else 0.0,
            passed,
            "order as expected" if passed else f"never reached {remaining}",
            {"called": called, "unmatched": remaining},
        )


class NoToolErrorEvaluator:
    type = "no_tool_error"
    needs_trace = True

    def __init__(self, name: str = "no_tool_error"):
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        if trace is None:
            return _ungradable(self.name, self.type, "no trace: tool errors unknown")
        errors = int(trace.get("tool_error_count") or 0)
        return EvaluatorResult(
            self.name,
            self.type,
            1.0 if errors == 0 else 0.0,
            errors == 0,
            "no tool errors" if errors == 0 else f"{errors} tool call(s) failed",
            {"tool_error_count": errors},
        )


class FunctionEvaluator:
    """Wrap a plain function, the interface the design documents.

    The shape is::

    def grade(task, trial, trace) -> dict
    """

    type = "function"
    needs_trace = True

    def __init__(
        self, fn: Callable[..., dict], name: str | None = None, needs_trace: bool = True
    ):
        self.fn = fn
        self.name = name or getattr(fn, "__name__", "function")
        self.needs_trace = needs_trace

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        raw = self.fn(task, trial, trace)
        return EvaluatorResult(
            self.name,
            self.type,
            float(raw.get("score", 0.0)),
            bool(raw.get("passed", False)),
            str(raw.get("reason", "")),
            dict(raw.get("assertions", {})),
        )


def _tool_names(trace: Trace) -> list[str]:
    raw = trace.get("tool_names")
    if isinstance(raw, list):
        return [str(t) for t in raw]
    try:
        parsed = json.loads(raw) if raw else []
    except (ValueError, TypeError):
        return []
    return [str(t) for t in parsed] if isinstance(parsed, list) else []


def run_evaluators(
    evaluators: Sequence[Evaluator], task: Task, trial: Trial, trace: Trace | None
) -> list[EvaluatorResult]:
    """Every evaluator, in order, none of which may break the run."""
    results: list[EvaluatorResult] = []
    for evaluator in evaluators:
        try:
            results.append(evaluator.grade(task, trial, trace))
        except Exception as err:  # noqa: BLE001 — a bad evaluator is not a bad agent
            results.append(
                EvaluatorResult(
                    getattr(evaluator, "name", "unknown"),
                    getattr(evaluator, "type", "unknown"),
                    0.0,
                    False,
                    f"evaluator raised: {err}",
                    ungradable=True,
                )
            )
    return results


def verdict(
    results: Sequence[EvaluatorResult],
    policy: PassPolicy | str = "all",
    threshold: float = 0.7,
) -> bool | None:
    """Whether the trial passed, under the suite's policy.

    Returns ``None`` when nothing was gradable, which is not the same as
    failing — it means the trial says nothing about the agent either way.

    ``all`` is the default and stays the default. The other two can turn a
    failing trial into a passing one, so a suite has to ask for them:

    - ``any`` — one evaluator passing is enough. For a task with several acceptable
      answers expressed as separate checks.
    - ``threshold`` — the mean score clears a bar. For rubric-led suites where
      partial credit is the measure and a hard pass/fail per evaluator is not.
    """
    gradable = [r for r in results if not r.ungradable]
    if not gradable:
        return None

    name = getattr(policy, "value", policy)
    if name == "any":
        return any(r.passed for r in gradable)
    if name == "threshold":
        return (sum(r.score for r in gradable) / len(gradable)) >= threshold
    return all(r.passed for r in gradable)


class SqlStateEvaluator:
    """Assert the world changed, not just that the agent said it did.

    An agent that answers "I've cancelled order 4471" convincingly and calls
    nothing is indistinguishable, on the text alone, from one that did the work.
    This runs a read query and compares one value against what the task expects.

    ``query`` is injected rather than opened here: the evaluator has no business
    holding credentials, and the job that runs it already has a feature store
    session. Without one the evaluator is ungradable — never passing, since "I
    could not check" must not read as "the state was right".
    """

    type = "sql_state"
    needs_trace = False

    def __init__(
        self,
        sql: str,
        expect: Any = None,
        *,
        query: Callable[[str], Any] | None = None,
        name: str = "sql_state",
    ):
        self.sql = sql
        self.expect = expect
        self.query = query
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        if self.query is None:
            return _ungradable(
                self.name,
                self.type,
                "no query function configured: state could not be checked",
            )
        try:
            actual = self.query(self.sql)
        except Exception as err:  # noqa: BLE001 — a broken query is not a broken agent
            return _ungradable(self.name, self.type, f"query failed: {err}")

        got = _scalar(actual)
        passed = str(got).strip() == str(self.expect).strip()
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=1.0 if passed else 0.0,
            passed=passed,
            reason="" if passed else f"expected {self.expect!r}, found {got!r}",
            assertions={"sql": self.sql, "expected": self.expect, "actual": got},
        )


def _scalar(result: Any) -> Any:
    """First cell of whatever the query returned.

    Accepts a DataFrame, a list of rows, or a bare value, because the caller's
    session decides the shape and the evaluator asserts one value either way.
    """
    if result is None:
        return None
    values = getattr(result, "values", None)
    if values is not None and hasattr(result, "columns"):  # DataFrame
        return values[0][0] if len(values) and len(values[0]) else None
    if isinstance(result, (list, tuple)):
        if not result:
            return None
        first = result[0]
        if isinstance(first, (list, tuple)):
            return first[0] if first else None
        return first
    return result


class HumanReviewEvaluator:
    """A verdict this run cannot produce, held open until a person gives one.

    Returns ungradable with ``awaiting_review``, which the runner reads to mark
    the trial ``AWAITING_REVIEW`` rather than letting the other evaluators decide
    it. That distinction is the point: a task that asked for human judgement and
    silently passed on a substring match has not been judged.

    The verdict arrives later through the review endpoint, which writes a real
    evaluator result alongside this one.
    """

    type = "human_review"
    needs_trace = False

    def __init__(self, prompt: str = "", name: str = "human_review"):
        self.prompt = prompt
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        result = _ungradable(
            self.name,
            self.type,
            self.prompt or "waiting for a reviewer to judge this answer",
        )
        result.assertions = {"awaiting_review": True, "prompt": self.prompt}
        return result


AWAITING_REVIEW = "awaiting_review"


def awaits_review(results: Sequence[EvaluatorResult]) -> bool:
    """Whether any evaluator deferred to a person."""
    return any(r.assertions.get(AWAITING_REVIEW) for r in results)


# ── tool-use evaluators ──────────────────────────────────────────────────────
#
# All of these read `trace["tool_calls"]`: the ordered list of TOOL spans with
# their arguments, results, status and duration. Two rules they share.
#
# **Missing instrumentation is ungradable, never a failure.** Whether a
# framework writes tool arguments onto its spans is a property of the
# framework, not of the agent. Scoring their absence as a failing trial would
# report a tracing gap as a misbehaving agent — the same mistake as scoring a
# missing trace zero.
#
# **A tool that ran is judged; a tool that did not is not.** A evaluator scoped to
# one tool that never appears returns ungradable rather than passing vacuously,
# because "it never called the tool" is a `tool_call` verdict and saying it
# twice in two evaluators makes one of them noise.


def _tool_calls(trace: Trace | None) -> list[dict[str, Any]] | None:
    if trace is None:
        return None
    calls = trace.get("tool_calls")
    return calls if isinstance(calls, list) else None


def _scoped(calls: list[dict[str, Any]], tool: str) -> list[dict[str, Any]]:
    return [c for c in calls if not tool or c.get("name") == tool]


class ToolArgumentEvaluator:
    """The arguments a tool was called with parse, and carry what they must.

    Deterministic shape checking only: that the payload is JSON, and that the
    keys someone said must be there are there. Whether the *values* make sense
    for the question asked is a judgement, and lives in ToolArgumentsJudge.
    """

    type = "tool_arguments"
    needs_trace = True

    def __init__(
        self,
        tool: str = "",
        required_keys: Sequence[str] = (),
        *,
        must_parse: bool = True,
        name: str = "tool_arguments",
    ):
        self.tool = tool
        self.required_keys = list(required_keys)
        self.must_parse = must_parse
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        calls = _tool_calls(trace)
        if calls is None:
            return _ungradable(self.name, self.type, "no trace: arguments unknown")
        scoped = _scoped(calls, self.tool)
        if not scoped:
            return _ungradable(
                self.name,
                self.type,
                f"{self.tool or 'no tool'} was never called; that is a tool_call verdict",
            )

        checked = 0
        problems: list[str] = []
        for call in scoped:
            raw = call.get("arguments") or ""
            if not raw:
                # instrumentation, not the agent
                continue
            checked += 1
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                if self.must_parse:
                    problems.append(f"{call.get('name')}: arguments are not JSON")
                continue
            if not isinstance(parsed, dict):
                if self.required_keys:
                    problems.append(f"{call.get('name')}: arguments are not an object")
                continue
            missing = [k for k in self.required_keys if k not in parsed]
            if missing:
                problems.append(f"{call.get('name')}: missing {', '.join(missing)}")

        if checked == 0:
            return _ungradable(
                self.name,
                self.type,
                "no tool call recorded its arguments; the framework does not "
                "appear to trace them",
            )
        passed = not problems
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=1.0 if passed else 0.0,
            passed=passed,
            reason="; ".join(problems[:5]),
            assertions={"calls_checked": checked, "problems": problems},
        )


class UnnecessaryToolEvaluator:
    """No tool ran that the task did not ask for.

    ``allowed`` defaults to the task's required tools, which makes this the
    complement of ToolCallEvaluator: that one asks whether everything needed
    happened, this asks whether anything else did. A task with no required
    tools and no explicit allowlist has nothing to say here and is ungradable —
    the alternative would fail every agent that used a tool at all.
    """

    type = "no_unnecessary_tools"
    needs_trace = True

    def __init__(self, allowed: Sequence[str] = (), name: str = "no_unnecessary_tools"):
        self.allowed = list(allowed)
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        calls = _tool_calls(trace)
        if calls is None:
            return _ungradable(self.name, self.type, "no trace: tool calls unknown")
        allowed = set(self.allowed or task.expects_list(self.name))
        if not allowed:
            return _ungradable(
                self.name,
                self.type,
                "the task names no tools, so there is nothing to call unnecessary",
            )
        extra = sorted(
            {
                c.get("name", "")
                for c in calls
                if c.get("name") and c.get("name") not in allowed
            }
        )
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=0.0 if extra else 1.0,
            passed=not extra,
            reason=f"called {', '.join(extra)}, which the task did not ask for"
            if extra
            else "",
            assertions={"unexpected": extra, "allowed": sorted(allowed)},
        )


class ToolRetryEvaluator:
    """The agent did not call the same tool the same way more than it should.

    A retry is the *same tool with the same arguments*, not merely the same
    tool twice: looking up two different orders is two calls, looking up the
    same order twice is a retry. Where arguments are not traced the evaluator
    falls back to counting repeats by name and says so, because a loose signal
    labelled as such beats a confident wrong one.
    """

    type = "tool_retries"
    needs_trace = True

    def __init__(
        self, max_retries: int = 0, tool: str = "", name: str = "tool_retries"
    ):
        self.max_retries = max_retries
        self.tool = tool
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        calls = _tool_calls(trace)
        if calls is None:
            return _ungradable(self.name, self.type, "no trace: retries unknown")
        scoped = _scoped(calls, self.tool)
        if not scoped:
            return _ungradable(self.name, self.type, "no tool calls to examine")

        by_arguments = all(c.get("arguments") for c in scoped)
        counts: dict[tuple[str, str], int] = {}
        for call in scoped:
            key = (
                call.get("name", ""),
                call.get("arguments", "") if by_arguments else "",
            )
            counts[key] = counts.get(key, 0) + 1

        repeats = {name: count - 1 for (name, _), count in counts.items() if count > 1}
        worst = max(repeats.values(), default=0)
        passed = worst <= self.max_retries
        basis = (
            "identical arguments" if by_arguments else "name only, arguments not traced"
        )
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=1.0 if passed else 0.0,
            passed=passed,
            reason=""
            if passed
            else (
                f"{', '.join(f'{n} repeated {c}x' for n, c in sorted(repeats.items()))} "
                f"(matched on {basis})"
            ),
            assertions={"retries": repeats, "matched_on": basis},
        )


class ToolLatencyEvaluator:
    """No tool call took longer than its budget.

    Judges the slowest single call rather than the total, because a budget is
    usually about one call blocking a turn. A call whose span never closed has
    no duration and is reported rather than treated as instant — an unclosed
    span is the shape a hung tool leaves behind.
    """

    type = "tool_latency"
    needs_trace = True

    def __init__(self, max_ms: float, tool: str = "", name: str = "tool_latency"):
        self.max_ms = float(max_ms)
        self.tool = tool
        self.name = name

    def grade(self, task: Task, trial: Trial, trace: Trace | None) -> EvaluatorResult:
        calls = _tool_calls(trace)
        if calls is None:
            return _ungradable(self.name, self.type, "no trace: durations unknown")
        scoped = _scoped(calls, self.tool)
        if not scoped:
            return _ungradable(self.name, self.type, "no tool calls to time")

        timed = [c for c in scoped if c.get("duration_ms") is not None]
        unclosed = [c.get("name", "") for c in scoped if c.get("duration_ms") is None]
        if not timed:
            return _ungradable(
                self.name,
                self.type,
                "no tool span carried both a start and an end time",
            )

        slowest = max(timed, key=lambda c: c["duration_ms"])
        over = slowest["duration_ms"] > self.max_ms
        reasons = []
        if over:
            reasons.append(
                f"{slowest.get('name')} took {slowest['duration_ms']:.0f}ms, "
                f"budget {self.max_ms:.0f}ms"
            )
        if unclosed:
            reasons.append(f"unfinished span for {', '.join(sorted(set(unclosed)))}")
        return EvaluatorResult(
            evaluator_name=self.name,
            evaluator_type=self.type,
            score=0.0 if (over or unclosed) else 1.0,
            passed=not over and not unclosed,
            reason="; ".join(reasons),
            assertions={
                "slowest_ms": slowest["duration_ms"],
                "slowest_tool": slowest.get("name"),
                "budget_ms": self.max_ms,
                "unclosed": sorted(set(unclosed)),
            },
        )
