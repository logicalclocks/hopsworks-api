"""Online evaluation: grade a sample of traffic that already happened.

Offline evaluation — a suite run — answers "does the agent pass the cases we
wrote down?". This answers "how is it doing on the cases users actually bring?",
which is the larger and less flattering set. Neither replaces the other: a suite
cannot contain a question nobody thought of, and production cannot tell you
whether a fix held, because the case that broke may not come back.

Two things follow from the traffic having already happened, and both simplify
the work:

  - **There is no agent to call.** The transcript and the trajectory are in the
    trace. So there is no request adapter, no traceparent to generate, and no
    trace readiness to wait for — the trace is why we are here.
  - **There is no expected answer**, because nobody wrote one. So the
    reference-based evaluators do not apply at all: `contains` has nothing to
    contain, `tool_call` has no required list. What is left is the checks that
    judge a response on its own terms — a rubric judge, and the trajectory
    checks that read only the trace.

That second point is why an online score and a suite pass rate are different
measurements and must not be averaged. The run row says which it is
(``runType``), and this module only ever produces ``ONLINE_SAMPLE``.

Driven by a run row like every other run, so it is started, recorded, reported
and logged through exactly the same path — and so a schedule on the deployment's
evaluation job can fire it with no new machinery.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .evaluators import Evaluator, Trace, verdict
from .models import PassPolicy, Task, TraceStatus, Trial, TrialStatus
from .run_fields import run_trials
from .runner import RunResult, _transcript


if TYPE_CHECKING:
    import random
    from collections.abc import Sequence


log = logging.getLogger(__name__)

#: How many trace summaries to pull before sampling. The window filter is
#: applied client-side, so this bounds the read rather than the sample.
LISTING_LIMIT = 500
#: How many earlier turns of the same conversation a judge is shown. Enough
#: that "you already told me" is checkable; bounded so a support chat that has
#: run all afternoon does not arrive as the whole afternoon.
CONTEXT_TURNS = 20
#: How many of a session's traces to ask for when reconstructing the turns before
#: the graded one. Newest first from the server, so the page has to cover the
#: turns after the graded one too before it reaches the ones before it.
SESSION_PAGE = 100


def evaluators_for(run: dict[str, Any], judge_completer: Any = None) -> list[Evaluator]:
    """The checks this monitor grades with, from the spec on the run.

    The same `evaluators_from_spec` a suite run uses. It is the same machinery on
    the same shape -- what differs is only where the inputs came from, which is
    the whole point: a check does not need to know whether the conversation it is
    reading was authored or served.

    The spec is on the run rather than looked up, so an evaluator edited after
    this started cannot change what it graded with halfway through.
    """
    from .evaluator_spec import evaluators_from_spec

    return list(
        evaluators_from_spec(
            run.get("sampleEvaluators") or "[]", judge_completer=judge_completer
        )
    )


def within_window(
    summaries: Sequence[dict[str, Any]], start_ms: float, end_ms: float
) -> list[dict[str, Any]]:
    """The summaries inside the window, half-open: (start, end].

    Half-open so consecutive runs partition the traffic rather than overlapping
    at the boundary. A trace graded by the run that ended at T must not be graded
    again by the run that starts at T -- monitoring counts each conversation
    once, and a double-counted trace moves a rate.
    """
    inside = []
    for summary in summaries:
        created = summary.get("createdAt")
        if created is None:
            # Its age is unknown, and treating unknown as inside would quietly
            # widen whatever window was asked for.
            continue
        if start_ms < created <= end_ms:
            inside.append(summary)
    return inside


def oldest_first(inside: Sequence[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Everything in the window, oldest first, up to the ceiling.

    Not a random sample any more. The window already says what to grade — since
    the last monitor, usually — so choosing a subset of it would mean deciding on
    someone's behalf how much of their own traffic to look at, and leaving the
    rest ungraded with nothing recording that.

    Oldest first because the ceiling has to cut somewhere, and cutting the newest
    leaves a contiguous ungraded block the watermark can be left in front of. The
    run then stops at the last trace it graded and the next one continues from
    there, so a ceiling defers work rather than skipping it.
    """
    ordered = sorted(inside, key=lambda s: s.get("createdAt") or 0)
    return ordered[:limit] if limit and limit > 0 else ordered


def question_and_answer(detail: dict[str, Any]) -> tuple[str, str]:
    """The user's question and the agent's final answer, from the trace.

    The messages are on whichever span carried them, so this takes the first
    span that has any and the last assistant turn within it — the same
    reconstruction the trace view does, and heuristic for the same reason.
    """
    for span in sorted(
        detail.get("spans") or [], key=lambda s: s.get("startTimeNs") or 0
    ):
        if not span.get("messages"):
            continue
        question, answer = turn_of(span.get("messages"))
        if question or answer:
            return question, answer
    return "", ""


def turn_of(messages_raw: Any) -> tuple[str, str]:
    """One trace's user question and final agent answer, from its messages."""
    try:
        messages = (
            json.loads(messages_raw) if isinstance(messages_raw, str) else messages_raw
        )
    except (ValueError, TypeError):
        return "", ""
    if not isinstance(messages, list):
        return "", ""
    question = next(
        (str(m.get("content", "")) for m in messages if m.get("role") == "user"), ""
    )
    answer = ""
    for message in messages:
        if message.get("role") == "assistant" and message.get("content"):
            answer = str(message["content"])
    return question, answer


def started_ns(summary: dict[str, Any], detail: dict[str, Any]) -> int:
    """When the graded turn began, for telling earlier turns from later ones."""
    if summary.get("startTimeNs"):
        return int(summary["startTimeNs"])
    starts = [int(s.get("startTimeNs") or 0) for s in detail.get("spans") or []]
    return min((t for t in starts if t), default=0)


def conversation_before(
    session: Any,
    base: str,
    session_id: str,
    before_ns: int,
    cache: dict[str, list[dict[str, Any]]],
) -> list[tuple[str, str]]:
    """The turns of this conversation that came before the graded one, oldest first.

    A judge asked whether an answer is hallucinated, relevant, or the third time
    the user has said their name cannot tell from the turn alone; the earlier
    turns are the reference. Live traffic has no task to carry them, but every
    trace names its session, and the session's traces are one request away.

    Only what came before: a judge shown a later turn would be grading with the
    future. Fetched once per session per run, since a conversation with five
    turns in the window would otherwise be fetched five times. A session that
    cannot be read grades without context rather than not at all -- the log says
    so, and the verdict is the one a single-turn judge would have given anyway.
    """
    if not session_id or not before_ns:
        return []
    if session_id not in cache:
        try:
            response = session.get(
                f"{base}/traces/sessions/{session_id}",
                params={"limit": SESSION_PAGE},
                timeout=60,
            )
            response.raise_for_status()
            cache[session_id] = list(response.json().get("items") or [])
        except Exception:  # noqa: BLE001 -- context is a help, not a requirement
            log.exception(
                "could not read session %s; grading its turns without the conversation",
                session_id,
            )
            cache[session_id] = []
    earlier = sorted(
        (
            t
            for t in cache[session_id]
            if 0 < int(t.get("startTimeNs") or 0) < before_ns
        ),
        key=lambda t: int(t.get("startTimeNs") or 0),
    )[-CONTEXT_TURNS:]
    spoken: list[tuple[str, str]] = []
    for turn in earlier:
        question, answer = turn_of(turn.get("messages"))
        spoken.extend((("user", question), ("agent", answer)))
    return spoken


def as_trial(
    run_id: str, deployment_id: int, trace_id: str, answer: str, trace: Trace | None
) -> Trial:
    """A sampled trace, as a trial, before it has been graded.

    The status is settled after grading, by the same `verdict` a suite run uses
    — see `grade_trace`. Leaving it at a fixed PASSED would make the run's pass
    rate exactly 1.0 every time, which is a confidently wrong number rather than
    an absent one.
    """
    return Trial(
        trial_id=f"{run_id}/{trace_id}",
        run_id=run_id,
        task_id=trace_id,
        task_version=1,
        trial_index=0,
        deployment_id=deployment_id,
        trace_id=trace_id,
        trace_status=TraceStatus.RECEIVED if trace else TraceStatus.MISSING,
        final_output=answer,
        tool_error_count=(trace or {}).get("tool_error_count"),
        tool_call_count=len((trace or {}).get("tool_calls") or []) or None,
        input_tokens=(trace or {}).get("input_tokens"),
        output_tokens=(trace or {}).get("output_tokens"),
        completed_at=datetime.now(tz=timezone.utc),
    )


def grade_trace(
    evaluators: Sequence[Evaluator],
    run_id: str,
    deployment_id: int,
    trace_id: str,
    detail: dict[str, Any],
    trace: Trace | None,
    earlier_turns: Sequence[tuple[str, str]] = (),
    session_id: str = "",
) -> Trial:
    """One sampled trace, graded.

    `earlier_turns` is the conversation so far, oldest first; with it the judge
    is shown the whole exchange up to and including this turn, the same way a
    conversation task in a suite is graded. Without it -- a first turn, or a
    trace with no session -- the judge sees the turn alone, which is all there is.
    """
    from .evaluators import run_evaluators

    question, answer = question_and_answer(detail)
    # The trace id is the task id: production has no task, and this is the only
    # identifier that leads back to what was actually said.
    #
    # No expectations: production carries none, which is why every check here has
    # to be able to reach a verdict from its own configuration. The server
    # refuses one that cannot, so reaching this point means they all can.
    task = Task(
        task_id=trace_id,
        input_messages=json.dumps([{"role": "user", "content": question}]),
    )
    trial = as_trial(run_id, deployment_id, trace_id, answer, trace)
    trial.session_id = session_id
    if earlier_turns:
        trial.transcript = _transcript(
            [*earlier_turns, ("user", question), ("agent", answer)]
        )
    trial.evaluator_results = list(run_evaluators(evaluators, task, trial, trace))

    # `all`, and not configurable: a sample has no suite to carry a pass policy,
    # and the checks here are few and unrelated -- a judge saying the answer was
    # poor is not offset by no tool having errored.
    #
    # None means nothing was gradable, which is not failure: a trace with no
    # judge configured and no tool calls says nothing about the agent either
    # way, and counting it as a failure would make an unconfigured sample look
    # like a broken agent.
    outcome = verdict(trial.evaluator_results, PassPolicy.ALL)
    trial.status = (
        TrialStatus.PASSED
        if outcome
        else TrialStatus.FAILED
        if outcome is False
        else TrialStatus.TRACE_MISSING
    )
    return trial


def graded_through(sampled: Sequence[dict[str, Any]], window_end_ms: float) -> float:
    """Where this run actually got to, which is where the next one starts.

    The end of the window when everything in it was graded, and the last graded
    trace's own time when the ceiling cut it short. Advancing to the window's end
    regardless would step over traces nobody looked at and nothing would ever
    report them.
    """
    if not sampled:
        return window_end_ms
    return max(float(s.get("createdAt") or 0) for s in sampled)


def run_sample(
    client: Any,
    session: Any,
    api_base: str,
    project_id: int,
    run: dict[str, Any],
    evaluators: Sequence[Evaluator],
    rng: random.Random | None = None,
) -> RunResult:
    """List, sample, grade.

    `client` is the same `HopsworksAgentClient` a suite run uses, for
    `fetch_trace` alone — the trace shape an evaluator expects is defined there,
    and building a second one here is how the two drift.
    """
    deployment_id = int(run["deploymentId"])
    started = datetime.now(tz=timezone.utc)
    base = (
        f"{api_base.rstrip('/')}/hopsworks-api/api/project/{project_id}"
        f"/otel/servings/{deployment_id}"
    )

    listing = session.get(f"{base}/traces", params={"limit": LISTING_LIMIT}, timeout=60)
    listing.raise_for_status()
    summaries = listing.json().get("items") or []
    # The window the run was created with: everything since the last monitor
    # stopped, unless someone asked for a specific range.
    start_ms = float(run.get("sampleFrom") or 0)
    end_ms = float(
        run.get("sampleTo") or datetime.now(tz=timezone.utc).timestamp() * 1000
    )
    recent = within_window(summaries, start_ms, end_ms)
    if not recent:
        log.info("no new traces between %s and %s", start_ms, end_ms)
        return RunResult(
            run_id=run["runId"],
            suite_id="",
            deployment_id=deployment_id,
            started_at=started,
            completed_at=datetime.now(tz=timezone.utc),
        )

    graded_limit = run_trials(run, 0)
    sampled = oldest_first(recent, graded_limit)
    if len(sampled) < len(recent):
        # Said plainly, because the alternative reading -- that this is all the
        # traffic there was -- is the one someone would otherwise take.
        log.warning(
            "%d traces in the window, grading the oldest %d; the rest stay for the "
            "next run, which will start where this one stops",
            len(recent),
            len(sampled),
        )
    else:
        log.info("grading %d traces in the window", len(sampled))

    trials = []
    sessions: dict[str, list[dict[str, Any]]] = {}
    for summary in sampled:
        trace_id = summary.get("traceId")
        if not trace_id:
            continue
        try:
            detail = session.get(f"{base}/traces/{trace_id}", timeout=60).json()
            trace = client.fetch_trace(trace_id)
        except Exception:  # noqa: BLE001 — one unreadable trace is not the run failing
            log.exception("could not read trace %s", trace_id)
            continue
        session_id = str(summary.get("sessionId") or "")
        earlier = conversation_before(
            session, base, session_id, started_ns(summary, detail), sessions
        )
        trials.append(
            grade_trace(
                evaluators,
                run["runId"],
                deployment_id,
                trace_id,
                detail,
                trace,
                earlier_turns=earlier,
                session_id=session_id,
            )
        )

    return RunResult(
        run_id=run["runId"],
        # No suite, and the empty string is the record of that rather than a
        # missing value: this run executed no suite because there was none.
        suite_id="",
        deployment_id=deployment_id,
        trials=trials,
        # A sample that graded nothing is not a failed run: an agent with no
        # traffic in the window is a normal state, and reporting it as FAILED
        # would page someone about a quiet night.
        status="SUCCEEDED",
        started_at=started,
        completed_at=datetime.now(tz=timezone.utc),
    )
