r"""Building evaluators from what a task declares.

Before this, evaluators were *inferred*: a rubric got a judge, an expected output
got a substring check, tools got the tool evaluators. That covers the common cases
and reaches none of the others — a task had no way to ask for a regex, a JSON
shape, or a state check, so four implemented evaluators could never run.

A spec is a JSON array stored on the suite:

    [{"type": "regex", "pattern": "^ORD-\\\\d{4}$"},
     {"type": "json_schema", "required_keys": ["order_id", "status"]}]

The spec lives on the suite rather than on each task, so every task in a suite
is measured the same way. There is no inference: checks derived from whatever a
task happened to declare would make two tasks in one suite incomparable, which
is the thing a suite exists to prevent.

**Nothing here executes caller-supplied code.** ``FunctionEvaluator`` is
deliberately absent: it wraps a Python callable, and resolving one from a
string in a task row would turn "author a task" into "run code in the job".
Deterministic Python evaluators remain available to code that builds a run
directly.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from .evaluators import (
    ContainsEvaluator,
    Evaluator,
    ExactMatchEvaluator,
    HumanReviewEvaluator,
    JsonSchemaEvaluator,
    NoToolErrorEvaluator,
    RegexEvaluator,
    SqlStateEvaluator,
    ToolArgumentEvaluator,
    ToolCallEvaluator,
    ToolLatencyEvaluator,
    ToolOrderEvaluator,
    ToolRetryEvaluator,
    UnnecessaryToolEvaluator,
)
from .judge_config import (
    JudgeConfigError,
    api_key_for,
    api_key_source,
    completer_for,
    parse_judge_config,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from .models import Suite


log = logging.getLogger(__name__)

# The types a task may name. Kept explicit rather than derived from a module
# scan so adding a evaluator is a decision rather than an accident.
SPEC_TYPES = (
    "exact_match",
    "contains",
    "regex",
    "json_schema",
    "tool_call",
    "tool_order",
    "no_tool_error",
    "tool_arguments",
    "no_unnecessary_tools",
    "tool_retries",
    "tool_latency",
    "tool_arguments_judge",
    "tool_result_used",
    "llm_judge",
    "pairwise",
    "sql_state",
    "human_review",
)


class SpecError(ValueError):
    """A spec that cannot be turned into a evaluator, with the reason."""


def _one(
    entry: dict[str, Any],
    *,
    judge_completer: Callable[[str], str] | None,
    query: Callable[[str], Any] | None,
) -> Evaluator | None:
    kind = str(entry.get("type") or "").strip()
    name = str(entry.get("name") or kind or "evaluator")

    if kind == "exact_match":
        return ExactMatchEvaluator(
            name=name, case_sensitive=bool(entry.get("case_sensitive", False))
        )
    if kind == "contains":
        return ContainsEvaluator(name=name, expected=entry.get("expected"))
    if kind == "regex":
        pattern = entry.get("pattern")
        if not pattern:
            raise SpecError("a regex evaluator needs a pattern")
        return RegexEvaluator(
            pattern=str(pattern),
            name=name,
            should_match=bool(entry.get("should_match", True)),
        )
    if kind == "json_schema":
        keys = entry.get("required_keys") or entry.get("requiredKeys") or []
        if not isinstance(keys, (list, tuple)) or not keys:
            raise SpecError("a json_schema evaluator needs required_keys")
        return JsonSchemaEvaluator(required_keys=[str(k) for k in keys], name=name)
    if kind == "tool_call":
        return ToolCallEvaluator(name=name)
    if kind == "tool_order":
        return ToolOrderEvaluator(name=name)
    if kind == "no_tool_error":
        return NoToolErrorEvaluator(name=name)
    if kind == "tool_arguments":
        keys = entry.get("required_keys") or entry.get("requiredKeys") or []
        return ToolArgumentEvaluator(
            tool=str(entry.get("tool") or ""),
            required_keys=[str(k) for k in keys],
            must_parse=bool(entry.get("must_parse", True)),
            name=name,
        )
    if kind == "no_unnecessary_tools":
        allowed = entry.get("allowed") or []
        return UnnecessaryToolEvaluator(allowed=[str(a) for a in allowed], name=name)
    if kind == "tool_retries":
        return ToolRetryEvaluator(
            max_retries=int(entry.get("max_retries", 0)),
            tool=str(entry.get("tool") or ""),
            name=name,
        )
    if kind == "tool_latency":
        budget = entry.get("max_ms")
        if budget is None:
            raise SpecError("a tool_latency evaluator needs max_ms")
        return ToolLatencyEvaluator(
            max_ms=float(budget), tool=str(entry.get("tool") or ""), name=name
        )
    if kind == "human_review":
        return HumanReviewEvaluator(prompt=str(entry.get("prompt") or ""), name=name)
    if kind == "sql_state":
        sql = entry.get("sql") or entry.get("query")
        if not sql:
            raise SpecError("a sql_state evaluator needs a sql query")
        return SqlStateEvaluator(
            sql=str(sql), expect=entry.get("expect"), query=query, name=name
        )

    if kind in ("llm_judge", "pairwise", "tool_arguments_judge", "tool_result_used"):
        # Imported here because judges pulls in the provider plumbing, and a
        # suite with no judge configured should not need it loaded.
        from .judges import (
            DEFAULT_MODEL,
            LlmJudgeEvaluator,
            PairwiseEvaluator,
            ToolArgumentsJudge,
            ToolResultUsedJudge,
        )

        # Every judged evaluator may bring its own provider, model and key, not
        # only the rubric one — there is no reason a pairwise comparison should
        # be stuck with the project default when a rubric judge is not.
        try:
            config = parse_judge_config(entry)
        except JudgeConfigError as err:
            raise SpecError(str(err)) from err

        completer = judge_completer
        if config.model or entry.get("provider"):
            api_key = api_key_for(config)
            if api_key:
                completer = completer_for(config, api_key)
            else:
                log.warning(
                    "no key for judge %r (%s provider); looked in %s. It is "
                    "skipped, and a task graded only by it reports nothing",
                    name,
                    config.provider,
                    api_key_source(config),
                )
                return None

        if completer is None:
            # Not an error: a project without a judge key still runs its
            # deterministic evaluators, and the trial reports what was skipped
            # rather than pretending the judgement happened.
            log.info("no judge configured; skipping %s evaluator %r", kind, name)
            return None

        model = config.model or DEFAULT_MODEL
        if kind == "llm_judge":
            return LlmJudgeEvaluator(completer, config, name=name)
        if kind == "tool_arguments_judge":
            return ToolArgumentsJudge(
                completer, tool=str(entry.get("tool") or ""), name=name, model=model
            )
        if kind == "tool_result_used":
            return ToolResultUsedJudge(completer, name=name, model=model)
        return PairwiseEvaluator(
            completer,
            reference=str(entry.get("reference") or ""),
            name=name,
            model=model,
        )

    raise SpecError(
        f"unknown evaluator type {kind!r}; expected one of {', '.join(SPEC_TYPES)}"
    )


def evaluators_from_spec(
    spec: str | Sequence[dict[str, Any]] | None,
    *,
    judge_completer: Callable[[str], str] | None = None,
    query: Callable[[str], Any] | None = None,
) -> list[Evaluator]:
    """Every evaluator a spec asks for.

    Raises :class:`SpecError` on anything malformed. The caller decides what a
    bad spec means — the backend refuses it at authoring time, so by the time a
    run reads one it should already be valid.
    """
    if not spec:
        return []
    entries: Any = spec
    if isinstance(spec, str):
        try:
            entries = json.loads(spec)
        except (ValueError, TypeError) as err:
            raise SpecError(f"not valid JSON: {err}") from err
    if not isinstance(entries, (list, tuple)):
        raise SpecError("a evaluator spec is an array of objects")

    evaluators: list[Evaluator] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise SpecError(f"entry {index + 1} is not an object")
        try:
            evaluator = _one(entry, judge_completer=judge_completer, query=query)
        except SpecError as err:
            raise SpecError(f"entry {index + 1}: {err}") from err
        if evaluator is not None:
            evaluators.append(evaluator)
    return evaluators


def validate_spec(spec: str | None) -> None:
    """Raise :class:`SpecError` unless every entry could be built.

    Uses stand-in judge and query callables so a spec naming those evaluators
    validates without a provider key or a database session — authoring a task
    must not depend on runtime configuration that only the job has.
    """
    evaluators_from_spec(
        spec, judge_completer=lambda _prompt: "", query=lambda _sql: None
    )


def flatten_rows(spec: str | Sequence[dict[str, Any]] | None):
    """Rows as the spec builder wants them: config merged in beside type and name.

    The API stores one row per check with everything but its type and name in a
    `config` object, so a suite is not rationed one column for all of them. The
    builder reads a flat entry, which is also what a person writes by hand, so
    the two shapes meet here rather than in every caller.
    """
    if not isinstance(spec, (list, tuple)):
        return spec
    entries = []
    for row in spec:
        if not isinstance(row, dict) or "config" not in row:
            entries.append(row)
            continue
        merged = dict(row)
        config = merged.pop("config") or {}
        merged.pop("position", None)
        if isinstance(config, str):
            try:
                config = json.loads(config) if config.strip() else {}
            except ValueError:
                config = {}
        if isinstance(config, dict):
            merged = {**config, **merged}
        if merged.get("name") is None:
            merged.pop("name", None)
        entries.append(merged)
    return entries


def evaluators_for_suite(
    suite: Suite,
    *,
    judge_completer: Callable[[str], str] | None = None,
    query: Callable[[str], Any] | None = None,
) -> list[Evaluator]:
    """The checks every task in this suite is graded by.

    There is no per-task variation and no inference. Both were removed together,
    because they are the same idea: if the checks are derived from what each
    task happens to declare, two tasks in one suite are measured differently and
    the suite's pass rate stops meaning anything. A suite names its checks, and
    every task is held to them.

    The checks are still parameterised by each task — a shared "contains" reads
    each task's own expected output — so tasks differ in substance while the
    measurement stays identical.
    """
    return evaluators_from_spec(
        flatten_rows(suite.evaluators), judge_completer=judge_completer, query=query
    )
