"""Suites, tasks, runs, trials — the shapes the runner passes around.

Plain dataclasses rather than the feature-store rows they eventually become:
the runner should be testable without a feature store, and the row shape is a
serialisation detail that belongs at the edge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Sequence


class ExecutionMode(str, Enum):
    """How dangerous it is to point this suite at a deployment.

    Running a tool-using agent executes real tools. A ``safety`` suite is full
    of injection and exfiltration attempts by construction, so pointing one at
    an agent whose tools can mutate production is not a configuration mistake,
    it is the whole incident.
    """

    READ_ONLY = "read_only"
    SANDBOXED = "sandboxed"
    # Deliberately unsupported in v1: it needs a per-suite allowlist of tools
    # the run may trigger, and a runner that enforces it.
    LIVE = "live"


class TrialStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    # Classified failures, kept apart so a broken pipeline is never read as a
    # broken agent.
    AGENT_ERROR = "AGENT_ERROR"
    TOOL_ERROR = "TOOL_ERROR"
    TIMEOUT = "TIMEOUT"
    EVALUATOR_ERROR = "EVALUATOR_ERROR"
    INFRA_ERROR = "INFRA_ERROR"
    INVALID_OUTPUT = "INVALID_OUTPUT"
    TRACE_MISSING = "TRACE_MISSING"
    # A evaluator deferred to a person and no verdict has been recorded yet. Not a
    # failure and not a pass: counting it either way answers a question nobody
    # has answered.
    AWAITING_REVIEW = "AWAITING_REVIEW"
    # An outcome, not inherently a failure: in a safety suite a block is the
    # desired result. Folding it into AGENT_ERROR would make guardrail
    # behaviour unattributable.
    BLOCKED_BY_GUARDRAIL = "BLOCKED_BY_GUARDRAIL"


class TraceStatus(str, Enum):
    RECEIVED = "RECEIVED"
    PARTIAL = "PARTIAL"
    MISSING = "MISSING"
    #: No trace was asked for, because the trial ended before it got that far:
    #: a guardrail blocked it, the agent errored, it timed out, or the harness
    #: did. Distinct from MISSING, which means a trace was awaited and never
    #: arrived -- the one that says the observability pipeline is broken.
    NOT_FETCHED = "NOT_FETCHED"


@dataclass
class Task:
    task_id: str
    input_messages: str
    task_version: int = 1
    task_type: str = "single_turn"
    #: What this task expects, keyed by the name of the check that reads it.
    #:
    #: One entry per check rather than four fixed fields. What counts as correct
    #: is a property of the pairing — "UB40" is the answer one check wants, an
    #: ordered tool list is what another wants — so a check reading something
    #: new needs no new field here, and two checks that both want an answer can
    #: each have their own.
    #:
    #: The value is free text whose meaning the check defines. `expects_text`,
    #: `expects_list` and `expects_tools` below are how a check reads one.
    expectations: dict[str, str] = field(default_factory=dict)
    category: str = ""
    tags: list[str] = field(default_factory=list)
    #: Who the task is asked as, sent as the request's ``subject`` on every turn.
    #: Empty asks anonymously. A task promoted from a production turn carries the
    #: subject that turn ran as, because a memory or personalisation failure only
    #: happens to someone the agent has memory of -- asked anonymously, its
    #: regression passes. The agent reads that person's memory and, under
    #: evaluation, writes only its own.
    subject: str = ""
    #: The request ``context`` the client sent, replayed as sent.
    context: dict[str, Any] = field(default_factory=dict)

    def _expects_text(self, name: str) -> str:
        """What this task expects of the named check, as written."""
        return (self.expectations.get(name) or "").strip()

    def _expects_list(self, name: str) -> list[str]:
        """The same value read as a comma-separated list, order preserved."""
        return [
            item.strip() for item in self._expects_text(name).split(",") if item.strip()
        ]

    def _expects_tools(self, name: str) -> tuple[list[str], list[str]]:
        """The required and forbidden lists a tool check reads.

        The one expectation that is two things, so it is stored as a JSON object
        when both directions are used. A bare comma-separated list still means
        the tools that must be called, which is what a hand-written CSV column
        holds and what someone types first.
        """
        import json

        raw = self._expects_text(name)
        if not raw.startswith("{"):
            # A bare list, as someone types it into a CSV column: the tools
            # that must be called, with a leading "!" for one that must not.
            # The skill has documented that spelling from the start; without
            # it, "refund_order, !delete_order" asked for a tool called
            # "!delete_order" and the check could never pass.
            required = []
            forbidden = []
            for item in self._expects_list(name):
                if item.startswith("!"):
                    banned = item[1:].strip()
                    if banned:
                        forbidden.append(banned)
                else:
                    required.append(item)
            return required, forbidden
        if raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except ValueError:
                return [], []
            if not isinstance(parsed, dict):
                return [], []
            required = parsed.get("required") or []
            forbidden = parsed.get("forbidden") or []
            return (
                [str(t) for t in required if str(t).strip()],
                [str(t) for t in forbidden if str(t).strip()],
            )
        return [], []

    def _turns(self) -> list[str]:
        """The user messages to send, in order.

        A single-turn task has one. A multi-turn task is the same array with
        several, sent one at a time with the agent's replies in between.

        Assistant messages are not returned. A multi-turn task carries the
        user's side only: interleaving replies would make [user, assistant,
        user] ambiguous between a two-turn script and a one-turn script with
        seeded history, and the two are graded differently. Seeding history is
        a separate feature and gets its own field when it arrives.
        """
        import json

        try:
            messages = json.loads(self.input_messages)
        except (ValueError, TypeError):
            return [self.input_messages]
        if not isinstance(messages, list):
            return []
        return [
            str(m.get("content", ""))
            for m in messages
            if isinstance(m, dict) and m.get("role") == "user"
        ]

    @property
    def asked(self) -> str:
        """What the user said, for a judge to read as the question.

        Every turn for a multi-turn task, because "the question" is the whole
        script when there are several. Never raises: a judge reporting a
        conversation ungradable because it has more than one turn would blame
        the agent for the format.
        """
        turns = self._turns()
        if len(turns) <= 1:
            return turns[0] if turns else ""
        return "\n".join(f"{i + 1}. {turn}" for i, turn in enumerate(turns))

    @property
    def prompt(self) -> str:
        """The single user message to send.

        Only meaningful for a single-turn task, and it says so: this used to
        return the *last* user message of whatever it was given, so a task with
        three turns sent the third, ran cleanly, and produced a pass rate for a
        conversation that never happened.
        """
        turns = self._turns()
        if len(turns) > 1:
            raise ValueError(
                f"task {self.task_id} has {len(turns)} user turns; use _turns() "
                "and run them in order"
            )
        return turns[0] if turns else ""


class PassPolicy(str, Enum):
    """How a trial's evaluators combine into one verdict.

    ALL is the default and the safe one. ANY and THRESHOLD exist because some
    suites genuinely want them — a capability suite where three phrasings are
    each acceptable, or a rubric suite where a mean score is the measure — but
    both can turn a failing trial into a passing one, so neither is a default.
    """

    ALL = "all"
    ANY = "any"
    THRESHOLD = "threshold"


@dataclass
class Suite:
    suite_id: str
    suite_version: int = 1
    name: str = ""
    # Descriptive only; nothing here reads them.
    tags: list[str] = field(default_factory=list)
    execution_mode: ExecutionMode = ExecutionMode.READ_ONLY
    # A suite of attacks: a guardrail block is the desired outcome rather than a
    # failure, and the suite must run sandboxed. This replaced a `type` of
    # "safety", which bundled the same meaning into a category you had to know.
    blocks_are_success: bool = False
    tasks: list[Task] = field(default_factory=list)
    # The checks every task in this suite is graded by, as a JSON array.
    #
    # On the suite rather than on each task: a suite is a measurement, and a
    # pass rate only means something if the measurement is constant. Per task,
    # "60% passed" would aggregate incomparable things — and pass_policy below
    # could not be a suite-level rule at all, since the things it combines have
    # to be the same for every task.
    #: The checks every task is graded by.
    #:
    #: Either the flat JSON array the spec builder reads, or the rows the API
    #: stores — one dict per check with its `config` held separately. Both are
    #: accepted because they are the same list either side of one storage
    #: decision, and a runner that only understood one of them would break the
    #: moment the other appeared.
    evaluators: str | list[dict] = ""
    pass_policy: PassPolicy = PassPolicy.ALL
    # Only read under THRESHOLD: the mean score every gradable evaluator must reach.
    pass_threshold: float = 0.7


@dataclass
class EvaluatorResult:
    evaluator_name: str
    evaluator_type: str
    score: float
    passed: bool
    reason: str = ""
    assertions: dict[str, Any] = field(default_factory=dict)
    # A evaluator that could not judge -- a trajectory evaluator with no trace, say.
    # Distinct from failing: an ungradable trial must not count against the
    # agent, and must not silently count for it either.
    ungradable: bool = False


@dataclass
class Trial:
    trial_id: str
    run_id: str
    task_id: str
    task_version: int
    trial_index: int
    deployment_id: int
    trace_id: str = ""
    #: The conversation every turn of this trial shared, for a multi-turn task.
    #: Empty for a single turn, which is a conversation of one and has nothing
    #: the trace id does not already say.
    session_id: str = ""
    #: The exchange as text, when there was more than one turn. What a judge
    #: reads to grade the conversation rather than its last answer.
    transcript: str = ""
    #: Defaults to NOT_FETCHED: a trial that returns before the trace is
    #: awaited never asked for one, and counting that as a missing trace failed
    #: runs in which nothing was missing.
    trace_status: TraceStatus = TraceStatus.NOT_FETCHED
    status: TrialStatus = TrialStatus.FAILED
    final_output: str = ""
    latency_ms: float = 0.0
    error_type: str = ""
    error_message: str = ""
    evaluator_results: list[EvaluatorResult] = field(default_factory=list)
    #: Whether a guardrail stopped this trial, whatever it was then graded as.
    #:
    #: Separate from ``status`` because in a safety suite the block *is* the
    #: pass, so the status stops recording that it happened at all -- and the
    #: guardrail block rate, which is meant to be the raw count, read 0% in
    #: exactly the suites where guardrails were doing the most work. In memory
    #: only, like the counts below: the rate it feeds is written as a metric
    #: row, not a trials column.
    guardrail_blocked: bool = False
    # Read off the trace for run-level tool metrics. In memory only: the trials
    # feature group has no column for them, and adding one needs a version bump
    # that would leave existing projects on the old schema. The rate they feed
    # is written as an ordinary key/value metric row instead.
    tool_error_count: int | None = None
    tool_call_count: int | None = None
    # Read off the trace's LLM spans. Previously written as a hard-coded zero,
    # which made token and cost dashboards report a confident, wrong number
    # rather than an obviously absent one.
    input_tokens: int | None = None
    output_tokens: int | None = None
    estimated_cost: float | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    completed_at: datetime | None = None


def _derive_trial_id(
    run_id: str, task_id: str, task_version: int, trial_index: int
) -> str:
    """Deterministic, so a crashed runner that retries overwrites the trial's.

    row instead of appending a second one. A random id here would turn every
    infrastructure hiccup into duplicated results and a wrong pass rate.
    """
    return f"{run_id}/{task_id}/{task_version}/{trial_index}"


#: Trials that say nothing about the agent, and so belong in no denominator.
#: ``INFRA_ERROR`` and ``EVALUATOR_ERROR`` are the harness failing; counting
#: them makes a flaky network look like a regression and sends someone to debug
#: a prompt. ``TRACE_MISSING`` is a trial nothing could grade, and
#: ``AWAITING_REVIEW`` is one nobody has graded yet -- counted as failures, a
#: suite with a human_review check could never pass its gate and its
#: "regressions" were reviewers who had not clicked.
UNGRADED_STATUSES = (
    TrialStatus.INFRA_ERROR,
    TrialStatus.EVALUATOR_ERROR,
    TrialStatus.TRACE_MISSING,
    TrialStatus.AWAITING_REVIEW,
)


def _gradable_trials(trials: Sequence[Trial]) -> list[Trial]:
    """Trials whose outcome says something about the agent."""
    return [t for t in trials if t.status not in UNGRADED_STATUSES]
