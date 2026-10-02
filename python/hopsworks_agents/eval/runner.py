"""Execute a suite against a deployed agent.

The runner is a *client* of the agent, which is what makes this design differ
from the SaaS eval tools: they run the task loop in the user's own process, so
they get output capture and trace correlation for free by being the caller.
Here the unit of evaluation is a deployed serving endpoint, so correlation,
trace readiness, and execution safety all have to be built rather than assumed.

Everything that touches the network is injected (:class:`AgentClient`), because
the parts worth testing are the refusals and the failure classification, not
the HTTP.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from .evaluators import Evaluator, Trace, awaits_review, run_evaluators, verdict
from .models import (
    ExecutionMode,
    Suite,
    Task,
    TraceStatus,
    Trial,
    TrialStatus,
    derive_trial_id,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


log = logging.getLogger(__name__)


class SuiteRefused(Exception):
    """The run was refused before any request was sent.

    Deliberately raised rather than returned: every one of these means the run
    would have produced results that look valid and are not, and a caller that
    ignores a return value would then act on them.
    """


@dataclass
class AgentResponse:
    text: str
    trace_id: str = ""
    blocked_by_guardrail: bool = False
    latency_ms: float = 0.0
    error: str = ""


class AgentClient(Protocol):
    """What the runner needs from the outside world."""

    def manifest(self) -> dict[str, Any]: ...

    def call(
        self, prompt: str, *, traceparent: str, baggage: str, timeout_s: float
    ) -> AgentResponse: ...

    def fetch_trace(self, trace_id: str) -> Trace | None: ...


@dataclass
class RunnerConfig:
    n_trials: int = 1
    timeout_s: float = 120.0
    max_concurrency: int = 4
    # Derived from the Stage 1 probe's trajectory-stable p95, not guessed. A
    # full suite is n_tasks x n_trials requests at a shared endpoint, so this
    # and max_concurrency are the difference between an eval run and a
    # self-inflicted load test on production capacity.
    readiness_timeout_s: float = 120.0
    readiness_poll_s: float = 2.0
    # Taken from the deployment's tracing config so an eval run is costed the
    # same way production traffic is. Absent means unpriced, which is reported
    # as such rather than as free.
    input_token_price_per_million: float | None = None
    output_token_price_per_million: float | None = None


@dataclass
class RunResult:
    run_id: str
    suite_id: str
    deployment_id: int
    trials: list[Trial] = field(default_factory=list)
    status: str = "SUCCEEDED"
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    completed_at: datetime | None = None


def check_deployment_supports(suite: Suite, manifest: dict[str, Any]) -> None:
    """Refuse a run that cannot produce trustworthy results.

    Both checks exist because the alternative is worse than an error: a run
    against an agent that ignores the traceparent writes trial rows pointing at
    traces that were never created, and a sandboxed suite against a live
    deployment executes injection attempts against real tools. Neither announces
    itself in the results.
    """
    capabilities = manifest.get("capabilities", {})

    if suite.execution_mode is ExecutionMode.LIVE:
        raise SuiteRefused(
            "execution_mode 'live' is not supported: it needs a per-suite "
            "allowlist of tools the run may trigger"
        )

    if "trace_correlation" not in capabilities:
        raise SuiteRefused(
            "the deployment's manifest does not report trace_correlation, so "
            "it is running an SDK too old to continue the runner's trace "
            "context; every trial would point at a trace that does not exist"
        )
    if not capabilities["trace_correlation"]:
        raise SuiteRefused(
            "tracing is disabled on this deployment, so no trial can be "
            "correlated to a trace"
        )

    # Either declaration will do. eval_mode is the whole deployment; eval_per_request
    # is the agent promising to read the hopsworks.eval.* baggage on each turn and
    # skip its production side effects for a verified run. Both are the author's
    # word — the runner can verify neither — so they are trusted alike.
    sandboxed_ok = capabilities.get("eval_mode") or capabilities.get("eval_per_request")
    if suite.execution_mode is ExecutionMode.SANDBOXED and not sandboxed_ok:
        raise SuiteRefused(
            f"suite {suite.suite_id} is sandboxed but the deployment reports neither "
            "eval_mode nor eval_per_request: its tools may still reach production "
            "systems"
        )
    if suite.blocks_are_success and suite.execution_mode is not ExecutionMode.SANDBOXED:
        raise SuiteRefused(
            "safety suites must be sandboxed: they contain injection and "
            "data-exfiltration attempts by construction"
        )


def _traceparent(trace_id: str, span_id: str) -> str:
    return f"00-{trace_id}-{span_id}-01"


def _baggage(
    run_id: str, suite: Suite, task: Task, trial_index: int, trial_id: str
) -> str:
    from hopsworks_agents.protocol import conventions

    return ",".join(
        [
            f"{conventions.EVAL_RUN_ID}={run_id}",
            f"{conventions.EVAL_SUITE_ID}={suite.suite_id}",
            f"{conventions.EVAL_SUITE_VERSION}={suite.suite_version}",
            f"{conventions.EVAL_TASK_ID}={task.task_id}",
            f"{conventions.EVAL_TASK_VERSION}={task.task_version}",
            f"{conventions.EVAL_TRIAL_ID}={trial_id}",
            f"{conventions.EVAL_TRIAL_INDEX}={trial_index}",
        ]
    )


def _new_ids() -> tuple[str, str]:
    import secrets

    return secrets.token_hex(16), secrets.token_hex(8)


def _classify(response: AgentResponse, suite: Suite) -> TrialStatus | None:
    """Turn a response into a failure class, or None when it is gradable."""
    if response.error:
        return (
            TrialStatus.INFRA_ERROR
            if "timeout" not in response.error.lower()
            else TrialStatus.TIMEOUT
        )
    if response.blocked_by_guardrail:
        return TrialStatus.BLOCKED_BY_GUARDRAIL
    return None


def _guardrail_outcome(suite: Suite) -> TrialStatus:
    """A guardrail block is an outcome, not inherently a failure.

    In a safety suite the block is the desired result and the trial passes; in
    a capability or regression suite it is an over-refusal and the trial fails.
    Measuring only the first is the classic mistake — guardrails look effective
    while quietly degrading the product.
    """
    if suite.blocks_are_success:
        return TrialStatus.PASSED
    return TrialStatus.BLOCKED_BY_GUARDRAIL


def _await_trace(
    client: AgentClient, trace_id: str, config: RunnerConfig, sleep=time.sleep
) -> tuple[Trace | None, TraceStatus]:
    """Poll until the trace is readable, or give up.

    Sidecar insert failures are logged and dropped, so a missing trace is a
    permanent condition to tolerate, not only a timing one.
    """
    if not trace_id:
        return None, TraceStatus.MISSING
    deadline = time.monotonic() + config.readiness_timeout_s
    while time.monotonic() < deadline:
        try:
            trace = client.fetch_trace(trace_id)
        except Exception:  # noqa: BLE001 — a transient read must not end the poll
            trace = None
        if trace:
            status = (
                TraceStatus.RECEIVED
                if trace.get("root_span_id")
                else TraceStatus.PARTIAL
            )
            return trace, status
        sleep(config.readiness_poll_s)
    return None, TraceStatus.MISSING


#: Task shapes the runner can execute. A type outside this is refused rather
#: than run as its nearest neighbour.
TASK_TYPES = ("single_turn", "multi_turn")


def _transcript(spoken: list[tuple[str, str]]) -> str:
    """The exchange as text, for a judge to read.

    Labelled by speaker and kept in order, because every conversation-level
    question -- was this resolved, did it contradict itself, was it told this
    already -- is a question about who said what and when.
    """
    return "\n\n".join(f"{who}: {text}" for who, text in spoken if text)


def _run_trial(
    client: AgentClient,
    suite: Suite,
    task: Task,
    trial_index: int,
    run_id: str,
    deployment_id: int,
    evaluators: Sequence[Evaluator],
    config: RunnerConfig,
    sleep=time.sleep,
) -> Trial:
    trial_id = derive_trial_id(run_id, task.task_id, task.task_version, trial_index)
    trace_id, span_id = _new_ids()
    trial = Trial(
        trial_id=trial_id,
        run_id=run_id,
        task_id=task.task_id,
        task_version=task.task_version,
        trial_index=trial_index,
        deployment_id=deployment_id,
        # stamped up front: a trial whose request fails is still correlated,
        # which is the reason the runner generates the id rather than reading
        # it back from the response
        trace_id=trace_id,
    )

    turns = task.turns()
    # One conversation for the whole script, so the agent's memory sees the
    # turns as one exchange -- which is the thing a multi-turn suite is testing.
    conversation_id = trial_id if len(turns) > 1 else None
    if conversation_id:
        trial.session_id = conversation_id
    spoken: list[tuple[str, str]] = []

    response = None
    for turn_index, text in enumerate(turns):
        # The trace of the last turn is the one the trajectory checks read: it
        # is where the answer was produced. Earlier turns still emit their own,
        # correlated by the conversation.
        last = turn_index == len(turns) - 1
        turn_trace, turn_span = (trace_id, span_id) if last else _new_ids()
        # Only for a conversation. A single turn calls exactly as it always
        # did, so an AgentClient written against the old signature -- the tests'
        # stubs, and anyone else's -- keeps working.
        threaded = {"conversation_id": conversation_id} if conversation_id else {}
        try:
            response = client.call(
                text,
                traceparent=_traceparent(turn_trace, turn_span),
                baggage=_baggage(run_id, suite, task, trial_index, trial_id),
                timeout_s=config.timeout_s,
                **threaded,
            )
        except Exception as err:  # noqa: BLE001
            trial.status = TrialStatus.INFRA_ERROR
            trial.error_type = type(err).__name__
            trial.error_message = str(err)
            trial.transcript = _transcript(spoken)
            trial.completed_at = datetime.now(tz=timezone.utc)
            return trial

        spoken.append(("user", text))
        spoken.append(("agent", response.text))

        # A turn that failed ends the conversation there. Sending the rest would
        # be answering questions the agent never asked, and grading a transcript
        # with a hole in it as though it were whole.
        if not last and (response.error or not response.text):
            trial.status = TrialStatus.FAILED
            trial.error_message = (
                response.error or f"no reply to turn {turn_index + 1} of {len(turns)}"
            )
            trial.transcript = _transcript(spoken)
            trial.completed_at = datetime.now(tz=timezone.utc)
            return trial

    if len(turns) > 1:
        trial.transcript = _transcript(spoken)

    trial.final_output = response.text if response else ""
    trial.latency_ms = response.latency_ms if response else 0.0

    failure = _classify(response, suite)
    if failure is TrialStatus.BLOCKED_BY_GUARDRAIL:
        trial.status = _guardrail_outcome(suite)
        trial.completed_at = datetime.now(tz=timezone.utc)
        return trial
    if failure is not None:
        trial.status = failure
        trial.error_message = response.error
        trial.completed_at = datetime.now(tz=timezone.utc)
        return trial

    trace, trace_status = _await_trace(
        client, response.trace_id or trace_id, config, sleep
    )
    trial.trace_status = trace_status

    if trace is not None:
        trial.tool_error_count = int(trace.get("tool_error_count") or 0)
        trial.tool_call_count = len(
            trace.get("tool_calls") or trace.get("tool_names") or []
        )
        trial.input_tokens = trace.get("input_tokens")
        trial.output_tokens = trace.get("output_tokens")
        trial.estimated_cost = _cost(trial, config)

    trial.evaluator_results = run_evaluators(evaluators, task, trial, trace)
    outcome = verdict(trial.evaluator_results, suite.pass_policy, suite.pass_threshold)

    if awaits_review(trial.evaluator_results):
        # A task that asked for human judgement has not been judged by the other
        # evaluators agreeing with each other. Held open rather than resolved, so a
        # reviewer's verdict is the thing that settles it.
        trial.status = TrialStatus.AWAITING_REVIEW
        trial.completed_at = datetime.now(tz=timezone.utc)
        return trial

    if trace_status is TraceStatus.MISSING:
        # Final-answer evaluators still ran on the captured response; only the
        # trajectory ones went ungradable. The trial keeps its answer verdict
        # but is marked so trajectory metrics can exclude it, and so a high
        # rate of these fails the run rather than the agent.
        trial.status = (
            TrialStatus.TRACE_MISSING
            if outcome is None
            else (TrialStatus.PASSED if outcome else TrialStatus.FAILED)
        )
        if outcome is None:
            trial.error_type = "TRACE_MISSING"
    else:
        trial.status = (
            TrialStatus.PASSED
            if outcome
            else TrialStatus.FAILED
            if outcome is False
            else TrialStatus.TRACE_MISSING
        )

    trial.completed_at = datetime.now(tz=timezone.utc)
    return trial


def _cost(trial: Trial, config: RunnerConfig) -> float | None:
    """What this trial cost, or None when nobody configured a price.

    None rather than 0.0 on purpose: a run with no pricing set up and a run that
    genuinely cost nothing are different facts, and a dashboard summing zeros
    reports the second when it means the first.
    """
    if (
        config.input_token_price_per_million is None
        and config.output_token_price_per_million is None
    ):
        return None
    return (
        (trial.input_tokens or 0) * (config.input_token_price_per_million or 0.0)
        + (trial.output_tokens or 0) * (config.output_token_price_per_million or 0.0)
    ) / 1_000_000


def run_suite(
    client: AgentClient,
    suite: Suite,
    *,
    run_id: str,
    deployment_id: int,
    evaluators: Sequence[Evaluator] | Callable[[Task], Sequence[Evaluator]],
    config: RunnerConfig | None = None,
    sleep=time.sleep,
) -> RunResult:
    """Execute every task, ``n_trials`` times each, and grade the results.

    ``evaluators`` may be one list applied to every task, or a function of the
    task. Per-task matters more than it looks: which evaluators apply depends on
    what a task declares, so one fixed list either judges a task on tools it
    never claimed to use, or judges nothing at all and reports a run that says
    nothing while looking complete.
    """
    config = config or RunnerConfig()
    check_deployment_supports(suite, client.manifest())

    unsupported = [t for t in suite.tasks if t.task_type not in TASK_TYPES]
    if unsupported:
        raise SuiteRefused(
            f"unsupported task types: {sorted({t.task_type for t in unsupported})}"
        )
    scripted = [
        t for t in suite.tasks if t.task_type == "single_turn" and len(t.turns()) > 1
    ]
    if scripted:
        # The failure this replaces: prompt returned the last user message, so
        # these ran their final turn alone and reported a pass rate for a
        # conversation that never happened.
        raise SuiteRefused(
            "these tasks have several user turns but are typed single_turn: "
            f"{sorted(t.task_id for t in scripted)}"
        )

    result = RunResult(
        run_id=run_id, suite_id=suite.suite_id, deployment_id=deployment_id
    )
    work = [
        (task, index)
        for task in suite.tasks
        for index in range(max(1, config.n_trials))
    ]

    # Bounded: a full suite against a shared endpoint is otherwise a
    # self-inflicted load test on production capacity.
    resolve = evaluators if callable(evaluators) else (lambda _task: evaluators)

    with ThreadPoolExecutor(max_workers=max(1, config.max_concurrency)) as pool:
        futures = [
            pool.submit(
                _run_trial,
                client,
                suite,
                task,
                index,
                run_id,
                deployment_id,
                resolve(task),
                config,
                sleep,
            )
            for task, index in work
        ]
        for future in futures:
            result.trials.append(future.result())

    result.trials.sort(key=lambda t: (t.task_id, t.trial_index))
    missing = sum(1 for t in result.trials if t.trace_status is TraceStatus.MISSING)
    if result.trials and missing / len(result.trials) > 0.5:
        # A high rate means the observability pipeline is broken, not the
        # agent. Passing the run would publish a pass rate computed mostly
        # from final answers while claiming to have judged trajectories.
        result.status = "FAILED"
        log.error("%d/%d trials never got a trace", missing, len(result.trials))
    result.completed_at = datetime.now(tz=timezone.utc)
    return result
