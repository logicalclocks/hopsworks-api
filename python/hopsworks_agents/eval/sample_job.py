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
import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .evaluators import Evaluator, Trace, _verdict
from .models import PassPolicy, Task, TraceStatus, Trial, TrialStatus
from .run_fields import _run_trials
from .runner import RunResult, _transcript


if TYPE_CHECKING:
    import random
    from collections.abc import Callable, Sequence


log = logging.getLogger(__name__)

#: How many trace summaries to pull before sampling. The window filter is
#: applied client-side, so this bounds the read rather than the sample.
#: How many graded conversations to hand a progress writer at a time. One write
#: per trace would be one feature-store insert per trace; one per run is what
#: left a stopped monitor with nothing.
WRITE_BATCH = 25

LISTING_LIMIT = 500
#: How many earlier turns of the same conversation a judge is shown. Enough
#: that "you already told me" is checkable; bounded so a support chat that has
#: run all afternoon does not arrive as the whole afternoon.
CONTEXT_TURNS = 20
#: How many of a session's traces to ask for when reconstructing the turns before
#: the graded one. Newest first from the server, so the page has to cover the
#: turns after the graded one too before it reaches the ones before it.
SESSION_PAGE = 100


def _evaluators_for(
    run: dict[str, Any], judge_completer: Any = None
) -> list[Evaluator]:
    """The checks this monitor grades with, from the spec on the run.

    The same `evaluators_from_spec` a suite run uses. It is the same machinery on
    the same shape -- what differs is only where the inputs came from, which is
    the whole point: a check does not need to know whether the conversation it is
    reading was authored or served.

    The spec is on the run rather than looked up, so an evaluator edited after
    this started cannot change what it graded with halfway through.
    """
    from .evaluator_spec import _evaluators_from_spec

    return list(
        _evaluators_from_spec(
            run.get("sampleEvaluators") or "[]", judge_completer=judge_completer
        )
    )


def _summary_ms(summary: dict[str, Any]) -> float | None:
    """When a listed conversation started, in epoch milliseconds.

    A trace summary is an `OtelSpanDTO`, and the field it carries is
    `startTimeNs`. `createdAt` is the UI's own derived field and never arrives
    from the backend, so keying the window on it skipped every summary as "age
    unknown": the monitor graded nothing, ended SUCCEEDED, and moved its
    watermark over the traffic. Still read first, so a caller that does supply
    it -- the tests, and any producer of already-milliseconds rows -- is not
    second-guessed.
    """
    created = summary.get("createdAt")
    if created is not None:
        return float(created)
    started = summary.get("startTimeNs")
    if started:
        return int(started) / 1e6
    return None


def _within_window(
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
        if summary.get("evalRunId"):
            # An evaluation trial, not production traffic. The listing already leaves these out
            # when the backend understands `excludeEval`; this is the same rule for one that
            # does not.
            continue
        created = _summary_ms(summary)
        if created is None:
            # Its age is unknown, and treating unknown as inside would quietly
            # widen whatever window was asked for.
            continue
        if start_ms < created <= end_ms:
            inside.append(summary)
    return inside


def _oldest_first(inside: Sequence[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
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
    ordered = sorted(inside, key=lambda s: _summary_ms(s) or 0)
    return ordered[:limit] if limit and limit > 0 else ordered


def _question_and_answer(detail: dict[str, Any]) -> tuple[str, str]:
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
        question, answer = _turn_of(span.get("messages"))
        if question or answer:
            return question, answer
    return "", ""


def _turn_of(messages_raw: Any) -> tuple[str, str]:
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


def _started_ns(summary: dict[str, Any], detail: dict[str, Any]) -> int:
    """When the graded turn began, for telling earlier turns from later ones."""
    if summary.get("startTimeNs"):
        return int(summary["startTimeNs"])
    starts = [int(s.get("startTimeNs") or 0) for s in detail.get("spans") or []]
    return min((t for t in starts if t), default=0)


def _conversation_before(
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
        question, answer = _turn_of(turn.get("messages"))
        spoken.extend((("user", question), ("agent", answer)))
    return spoken


def _as_trial(
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


def _grade_trace(
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
    from .evaluators import _run_evaluators

    question, answer = _question_and_answer(detail)
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
    trial = _as_trial(run_id, deployment_id, trace_id, answer, trace)
    trial.session_id = session_id
    if earlier_turns:
        trial.transcript = _transcript(
            [*earlier_turns, ("user", question), ("agent", answer)]
        )
    trial.evaluator_results = list(_run_evaluators(evaluators, task, trial, trace))

    # `all`, and not configurable: a sample has no suite to carry a pass policy,
    # and the checks here are few and unrelated -- a judge saying the answer was
    # poor is not offset by no tool having errored.
    #
    # None means nothing was gradable, which is not failure: a trace with no
    # judge configured and no tool calls says nothing about the agent either
    # way, and counting it as a failure would make an unconfigured sample look
    # like a broken agent.
    outcome = _verdict(trial.evaluator_results, PassPolicy.ALL)
    trial.status = (
        TrialStatus.PASSED
        if outcome
        else TrialStatus.FAILED
        if outcome is False
        else TrialStatus.TRACE_MISSING
    )
    return trial


def _graded_through(sampled: Sequence[dict[str, Any]], window_end_ms: float) -> float:
    """Where this run actually got to, which is where the next one starts.

    The end of the window when everything in it was graded, and the last graded
    trace's own time when the ceiling cut it short. Advancing to the window's end
    regardless would step over traces nobody looked at and nothing would ever
    report them.
    """
    if not sampled:
        return window_end_ms
    return max(_summary_ms(s) or 0 for s in sampled)


def _run_sample(
    client: Any,
    session: Any,
    api_base: str,
    project_id: int,
    run: dict[str, Any],
    evaluators: Sequence[Evaluator],
    rng: random.Random | None = None,
    strategy: str = "oldest",
    shared: bool = True,
    on_progress: Callable[[list[Trial]], None] | None = None,
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

    # The window the run was created with: everything since the last monitor
    # stopped, unless someone asked for a specific range.
    start_ms = float(run.get("sampleFrom") or 0)
    end_ms = float(
        run.get("sampleTo") or datetime.now(tz=timezone.utc).timestamp() * 1000
    )
    graded_limit = _run_trials(run, 0)
    # The window, in SQL, oldest first. Asking for "the newest 500 traces" and
    # filtering them here meant that above 500 in a window the monitor graded
    # the newest slice and skipped everything older -- and said nothing, because
    # the list it compared against had already been cut server-side. `count` is
    # what the window holds, so the run knows what it is leaving behind.
    listing = session.get(
        f"{base}/traces",
        params={
            "limit": graded_limit or LISTING_LIMIT,
            "from": int(start_ms),
            "to": int(end_ms),
            "order": "random" if strategy == "random" else "oldest",
            # Not the deployment's own suite runs. They are traffic it served, on the same
            # deployment, so a monitor grading "the last hour" graded a nightly suite's
            # questions as if customers had asked them -- and a safety suite's injection
            # attempts, which are meant to be refused and so drag a production pass rate down
            # by design. Filtered again below for a backend that does not know the parameter.
            "excludeEval": "true",
            # Seeded from the window, which every sample run of one execution
            # shares: each evaluator then draws the same conversations, so their
            # verdicts are about the same traffic and can be compared. It also
            # makes a re-run of a window grade what it graded before, which is
            # what you want when checking a fixed judge against the old one.
            **({"seed": int(start_ms)} if strategy == "random" and shared else {}),
        },
        timeout=60,
    )
    listing.raise_for_status()
    body = listing.json()
    summaries = body.get("items") or []
    # Filtered again here, and sorted again below: a cluster whose backend
    # predates the window parameters answers with the newest page of everything,
    # and grading that as if it were the window would be wrong rather than slow.
    recent = _within_window(summaries, start_ms, end_ms)
    available = body.get("count")
    if not isinstance(available, int) or available < len(recent):
        available = len(recent)
    if not recent:
        log.info("no new traces between %s and %s", start_ms, end_ms)
        return RunResult(
            run_id=run["runId"],
            suite_id="",
            deployment_id=deployment_id,
            started_at=started,
            completed_at=datetime.now(tz=timezone.utc),
        )

    # Random keeps what the server chose; oldest re-sorts, since a backend that
    # does not know the window parameters answered newest-first.
    # A ceiling of zero means "no ceiling", as it does everywhere else here --
    # `oldest_first` reads it that way. Random did not, so a run row without one
    # sliced the window to nothing and the monitor graded no traffic at all.
    sampled = (
        list(recent[:graded_limit] if graded_limit and graded_limit > 0 else recent)
        if strategy == "random"
        else _oldest_first(recent, graded_limit)
    )
    if available > len(sampled):
        # Said plainly, because the alternative reading -- that this is all the
        # traffic there was -- is the one someone would otherwise take.
        log.warning(
            "%d conversations in the window, grading %s %d%s",
            available,
            "a random" if strategy == "random" else "the oldest",
            len(sampled),
            ""
            if strategy == "random"
            else "; the rest stay for the next run, which starts where this one stops",
        )
    else:
        log.info("grading %d conversations in the window", len(sampled))

    trials: list[Trial] = []
    reported = 0
    sessions: dict[str, list[dict[str, Any]]] = {}
    for summary in sampled:
        trace_id = summary.get("traceId")
        if not trace_id:
            continue
        try:
            detail = session.get(f"{base}/traces/{trace_id}", timeout=60).json()
            trace = client._fetch_trace(trace_id)
        except Exception:  # noqa: BLE001 — one unreadable trace is not the run failing
            log.exception("could not read trace %s", trace_id)
            continue
        session_id = str(summary.get("sessionId") or "")
        earlier = _conversation_before(
            session, base, session_id, _started_ns(summary, detail), sessions
        )
        trials.append(
            _grade_trace(
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
        # Written as they are graded, so a monitor that is stopped half way
        # through a long window leaves what it graded rather than nothing.
        if on_progress is not None and len(trials) - reported >= WRITE_BATCH:
            try:
                on_progress(trials[reported:])
            except Exception:  # noqa: BLE001 -- a failed write is not a failed run
                log.exception(
                    "could not record %d graded conversations", len(trials) - reported
                )
            reported = len(trials)

    if on_progress is not None and len(trials) > reported:
        try:
            on_progress(trials[reported:])
        except Exception:  # noqa: BLE001 -- a failed write is not a failed run
            log.exception(
                "could not record the last %d graded conversations",
                len(trials) - reported,
            )

    # Where this run stopped, when it stopped short: the newest conversation it
    # graded, so the next window opens there and the ones it left are graded
    # rather than skipped. Reported only then -- a run that covered its whole
    # window leaves the end where it was asked to stop.
    processed_through = None
    if strategy != "random" and available > len(sampled) and sampled:
        # Past the newest one graded, not at it. The backend stores this as the
        # next window's start and filters on `start_time_ns > fromMs * 1e6`,
        # and `report` truncates the float to an int -- so a conversation whose
        # start carried a fraction of a millisecond fell inside the next window
        # again and was graded a second time, as a second trial row, counted
        # twice.
        newest = max(_summary_ms(summary) or 0 for summary in sampled)
        processed_through = math.floor(newest) + 1
    # Every trace in the window unreadable is not a quiet night, it is a broken read. Reported
    # SUCCEEDED it moved the watermark over all of them, so the traffic nobody graded was never
    # offered again -- the one case where "no trials" must not be taken for "no traffic".
    unreadable = bool(sampled) and not trials
    if unreadable:
        processed_through = None
    return RunResult(
        run_id=run["runId"],
        # No suite, and the empty string is the record of that rather than a
        # missing value: this run executed no suite because there was none.
        suite_id="",
        deployment_id=deployment_id,
        trials=trials,
        processed_through_ms=processed_through,
        available=available,
        # A sample that graded nothing is not a failed run: an agent with no
        # traffic in the window is a normal state, and reporting it as FAILED
        # would page someone about a quiet night.
        status="FAILED" if unreadable else "SUCCEEDED",
        error_message=(
            f"none of the {len(sampled)} conversations in the window could be read"
            if unreadable
            else ""
        ),
        started_at=started,
        completed_at=datetime.now(tz=timezone.utc),
    )
