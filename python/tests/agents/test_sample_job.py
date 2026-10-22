"""Online evaluation: sampling production traffic and grading it.

These exist because the module had none, and rotted silently for it. Three
things moved under it between being written and being run — `Task` lost
`rubric` for `expectations`, judge keys stopped coming from a secrets API, and
a job stopped having an API key — and every one of them was a `TypeError` or a
`KeyError` on the first trace, in a job nobody would look at until someone
asked why the dashboard was empty.

So the seams tested here are the ones that break when something moves
underneath: constructing a `Task` and a `Trial`, what the judge grades against,
and what the trial's status becomes.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

from hopsworks_agents.eval.evaluators import EvaluatorResult
from hopsworks_agents.eval.models import TraceStatus, TrialStatus
from hopsworks_agents.eval.sample_job import (
    _as_trial,
    _evaluators_for,
    _grade_trace,
    _oldest_first,
    _question_and_answer,
    _run_sample,
    _within_window,
)


#: A judge that grades against its own criteria, which is what makes a check
#: usable on production traffic — the server refuses one that cannot.
JUDGE_SPEC = json.dumps(
    [
        {
            "type": "llm_judge",
            "name": "faithfulness",
            "criteria": {"supported": {"weight": 1}},
        }
    ]
)
TRACE_ONLY_SPEC = json.dumps([{"type": "no_tool_error", "name": "no_tool_errored"}])


def with_judge(judge):
    return _evaluators_for({"sampleEvaluators": JUDGE_SPEC}, judge_completer=judge)


def trace_only():
    return _evaluators_for({"sampleEvaluators": TRACE_ONLY_SPEC})


#: A fixed reference for the window tests, which are handed their own `now`.
NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)


def summary(trace_id: str, hours_ago: float, since: datetime | None = None) -> dict:
    """A trace summary, that many hours before `since`.

    Defaults to real now rather than a fixed date: building these against one made
    every run_sample test pass on the day they were written and fail two days
    later. A test that expires is worse than no test, because it fails somewhere
    unrelated to whatever broke.

    The shape is the backend's: a trace summary is an `OtelSpanDTO` and carries
    `startTimeNs`, nothing else about time. These fabricated a `createdAt` the
    UI computes for itself, which is how the whole job could key its window on
    a field no cluster ever sends and still pass its tests.
    """
    when = (since or datetime.now(tz=timezone.utc)) - timedelta(hours=hours_ago)
    return {"traceId": trace_id, "startTimeNs": int(when.timestamp() * 1e9)}


def ms(hours_ago: float) -> float:
    return (
        datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
    ).timestamp() * 1000


def detail(question: str = "who sings this?", answer: str = "UB40") -> dict:
    return {
        "spans": [
            {
                "spanId": "root",
                "startTimeNs": 1,
                "messages": json.dumps(
                    [
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": answer},
                    ]
                ),
            },
        ]
    }


class StubJudge:
    """A completer returning the shape the judge actually asks for.

    Per-criterion scores, because that is what `_output_shape` prompts for and
    what the judge refuses to guess at: a judge that answered with one bare
    number is treated as ungradable rather than scored, so a stub returning one
    would pass this file while telling us nothing about the real path.
    """

    def __init__(self, score: float = 5.0, criterion: str = "overall"):
        self.prompts: list[str] = []
        self._score = score
        self._criterion = criterion

    def __call__(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return json.dumps(
            {
                "scores": {self._criterion: self._score},
                "reasoning": {self._criterion: "because"},
            }
        )


class TestChoosingWhatToGrade:
    def test_only_traces_inside_the_window_are_candidates(self):
        summaries = [summary("old", 100, NOW), summary("new", 1, NOW)]
        window = _within_window(summaries, ms(24) - 0, ms(0))
        assert [
            s["traceId"]
            for s in _within_window(
                summaries,
                (NOW - timedelta(hours=24)).timestamp() * 1000,
                NOW.timestamp() * 1000,
            )
        ] == ["new"]
        assert window is not None

    def test_the_window_is_half_open_so_runs_do_not_overlap(self):
        # a trace graded by the run that ended at T must not be graded again by
        # the run that starts at T: monitoring counts each conversation once
        boundary = NOW.timestamp() * 1000
        at_boundary = {"traceId": "edge", "startTimeNs": int(boundary * 1e6)}
        assert _within_window([at_boundary], boundary, boundary + 1000) == []
        assert _within_window([at_boundary], boundary - 1000, boundary) == [at_boundary]

    def test_the_window_reads_the_field_the_backend_actually_sends(self):
        # The whole feature turned on this: summaries are OtelSpanDTO rows with
        # startTimeNs and no createdAt, so keying the window on createdAt
        # skipped every one of them as "age unknown". The run then ended
        # SUCCEEDED with no trials and moved its watermark over the traffic,
        # which reads as a quiet night rather than as a monitor grading nothing.
        row = {"traceId": "t", "startTimeNs": int(NOW.timestamp() * 1e9)}
        window = _within_window(
            [row],
            (NOW - timedelta(hours=1)).timestamp() * 1000,
            (NOW + timedelta(hours=1)).timestamp() * 1000,
        )
        assert [s["traceId"] for s in window] == ["t"]

    def test_an_evaluation_trial_is_not_production_traffic(self):
        # A suite run is traffic this deployment served, on the same deployment, so a monitor
        # grading "the last hour" graded a nightly suite's questions as if customers had asked
        # them -- a safety suite's injection attempts included, which are meant to be refused.
        trial = {**summary("t", 1, NOW), "evalRunId": "run-9"}
        window = _within_window(
            [trial, summary("real", 1, NOW)],
            (NOW - timedelta(hours=2)).timestamp() * 1000,
            NOW.timestamp() * 1000,
        )
        assert [s["traceId"] for s in window] == ["real"]

    def test_a_trace_with_no_timestamp_is_not_guessed_into_the_window(self):
        # its age is unknown, and treating unknown as recent would quietly widen
        # whatever window someone asked for
        assert _within_window([{"traceId": "t"}], ms(24), ms(0)) == []

    def test_everything_in_the_window_is_graded(self):
        # the window already says what is new, so choosing a subset of it would
        # decide on someone's behalf how much of their own traffic to look at
        summaries = [summary(str(i), i, NOW) for i in range(20)]
        assert len(_oldest_first(summaries, 0)) == 20

    def test_a_ceiling_cuts_the_newest_not_the_middle(self):
        # so what is left ungraded is contiguous and the watermark can stop in
        # front of it -- a ceiling defers work rather than skipping it
        summaries = [summary(str(i), i, NOW) for i in range(10)]
        graded = _oldest_first(summaries, 3)
        assert [s["traceId"] for s in graded] == ["9", "8", "7"]

    def test_asking_for_more_than_exists_grades_everything(self):
        summaries = [summary("a", 1, NOW), summary("b", 2, NOW)]
        assert len(_oldest_first(summaries, 50)) == 2


class TestReadingTheTranscript:
    def test_the_question_and_the_final_answer_come_off_the_trace(self):
        assert _question_and_answer(detail()) == ("who sings this?", "UB40")

    def test_the_last_assistant_turn_wins(self):
        # an agent that spoke twice said the second thing to the customer
        trace = {
            "spans": [
                {
                    "spanId": "r",
                    "startTimeNs": 1,
                    "messages": json.dumps(
                        [
                            {"role": "user", "content": "q"},
                            {"role": "assistant", "content": "first"},
                            {"role": "assistant", "content": "second"},
                        ]
                    ),
                }
            ]
        }
        assert _question_and_answer(trace)[1] == "second"

    def test_an_unparseable_span_is_skipped_rather_than_fatal(self):
        trace = {
            "spans": [
                {"spanId": "a", "startTimeNs": 1, "messages": "not json"},
                {
                    "spanId": "b",
                    "startTimeNs": 2,
                    "messages": json.dumps([{"role": "user", "content": "q"}]),
                },
            ]
        }
        assert _question_and_answer(trace)[0] == "q"

    def test_a_trace_carrying_no_messages_is_empty_rather_than_an_error(self):
        assert _question_and_answer({"spans": [{"spanId": "a"}]}) == ("", "")


class TestWhatCanGradeProductionTraffic:
    def test_the_checks_come_from_the_run_rather_than_being_hardcoded(self):
        # the same evaluators_from_spec a suite run uses: a check does not need to
        # know whether the conversation it reads was authored or served
        kinds = [e.type for e in with_judge(StubJudge())]
        assert kinds == ["llm_judge"]

    def test_a_trace_only_check_needs_no_judge_at_all(self):
        [only] = trace_only()
        assert only.type == "no_tool_error"

    def test_an_empty_spec_grades_with_nothing(self):
        # the server refuses this, so reaching it means something went wrong
        # upstream -- reporting nothing beats inventing a default check
        assert _evaluators_for({"sampleEvaluators": "[]"}) == []


class TestGradingOneTrace:
    def test_the_judges_criteria_are_what_it_grades_against(self):
        # a criteria-based judge needs nothing from the task, which is exactly
        # what makes it usable where nobody wrote an expected answer
        judge = StubJudge(criterion="supported")
        trial = _grade_trace(with_judge(judge), "run1", 3, "t1", detail(), None)
        assert "supported" in judge.prompts[0]
        assert trial.trial_id == "run1/t1"
        assert not any(r.ungradable for r in trial.evaluator_results)

    def test_the_task_carries_no_expectations_at_all(self):
        # production has none, and inventing one would anchor the judge on an
        # answer nobody wrote
        judge = StubJudge(criterion="supported")
        _grade_trace(with_judge(judge), "r", 1, "t", detail(), None)
        assert "expected" not in judge.prompts[0].lower().split("<user")[0]

    def test_the_verdict_settles_the_trial_status(self):
        # a fixed PASSED would make every sample report a pass rate of exactly
        # 1.0, which is a confidently wrong number rather than an absent one
        trace = {"trace_id": "t", "tool_calls": [], "tool_error_count": 0}
        passing = _grade_trace(
            with_judge(StubJudge(5.0, "supported")), "r", 1, "t", detail(), trace
        )
        failing = _grade_trace(
            with_judge(StubJudge(1.0, "supported")), "r", 1, "t2", detail(), trace
        )
        assert passing.status is TrialStatus.PASSED
        assert failing.status is TrialStatus.FAILED

    def test_a_judge_that_answers_with_one_bare_number_is_ungradable(self):
        # scoring it anyway would blame the agent for the judge -- and a stub
        # returning that shape is how this file could have passed while the real
        # path produced nothing
        class Bare:
            def __call__(self, prompt: str) -> str:
                return json.dumps({"score": 5.0})

        trial = _grade_trace(with_judge(Bare()), "r", 1, "t", detail(), None)
        assert all(r.ungradable for r in trial.evaluator_results)

    def test_nothing_gradable_is_not_counted_as_a_failure(self):
        # no judge configured and no trace: the trial says nothing about the
        # agent either way, and calling that a failure makes an unconfigured
        # sample look like a broken agent
        trial = _grade_trace([], "r", 1, "t", detail(), None)
        assert trial.status is not TrialStatus.FAILED

    def test_a_graded_trace_records_that_it_had_one(self):
        trace = {
            "trace_id": "t",
            "tool_calls": [],
            "tool_error_count": 0,
            "input_tokens": 10,
            "output_tokens": 4,
        }
        trial = _as_trial("r", 1, "t", "answer", trace)
        assert trial.trace_status is TraceStatus.RECEIVED
        assert (trial.input_tokens, trial.output_tokens) == (10, 4)


class FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, summaries, details, sessions=None, count=None):
        self._summaries = summaries
        self._details = details
        # session id -> the traces the session endpoint would list, any order
        self._sessions = sessions or {}
        self.session_fetches = 0
        # what the window holds, as the server reports it; None for a backend
        # that does not know the window parameters
        self._count = count
        self.listing_params = None

    def get(self, url, params=None, timeout=None):
        if url.endswith("/traces"):
            self.listing_params = params
            body = {"items": self._summaries}
            if self._count is not None:
                body["count"] = self._count
            return FakeResponse(body)
        if url.endswith("/traces/sessions"):
            # The session id travels as a query parameter: a trial's id has
            # slashes in it, so in the path it was a different route.
            self.session_fetches += 1
            asked = (params or {}).get("sessionId", "")
            return FakeResponse({"items": self._sessions.get(asked, [])})
        return FakeResponse(self._details[url.rsplit("/", 1)[-1]])


class FakeClient:
    def __init__(self, traces=None, fails=()):
        self._traces = traces or {}
        self._fails = set(fails)

    def _fetch_trace(self, trace_id):
        if trace_id in self._fails:
            raise RuntimeError("unreadable")
        return self._traces.get(trace_id)


def sample_run(**overrides):
    run = {
        "runId": "run1",
        "deploymentId": 3,
        "nTrials": 500,
        "sampleFrom": ms(48),
        "sampleTo": ms(0) + 60_000,
        "sampleEvaluators": TRACE_ONLY_SPEC,
    }
    run.update(overrides)
    return run


class TestARunOverProduction:
    def test_every_sampled_trace_becomes_a_trial(self):
        session = FakeSession(
            [summary("a", 1), summary("b", 2)], {"a": detail(), "b": detail()}
        )
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), with_judge(StubJudge())
        )
        assert {t.task_id for t in result.trials} == {"a", "b"}
        assert result.status == "SUCCEEDED"

    def test_a_run_over_production_names_no_suite(self):
        # nothing was executed against a frozen set of cases, and an empty suite
        # id is the record of that rather than a missing value
        session = FakeSession([summary("a", 1)], {"a": detail()})
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )
        assert result.suite_id == ""

    def test_a_quiet_window_is_not_a_failed_run(self):
        # an agent with no traffic overnight is a normal state; reporting it as
        # FAILED would page someone about it
        session = FakeSession([summary("old", 500)], {})
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )
        assert (result.status, result.trials) == ("SUCCEEDED", [])

    def test_one_unreadable_trace_does_not_end_the_run(self):
        session = FakeSession(
            [summary("a", 1), summary("b", 1)], {"a": detail(), "b": detail()}
        )
        result = _run_sample(
            FakeClient(fails={"a"}), session, "https://h", 1, sample_run(), trace_only()
        )
        assert [t.task_id for t in result.trials] == ["b"]

    def test_the_ceiling_comes_off_the_run_row(self):
        # the row is what the job is handed, so a scheduled monitor and a
        # hand-started one cannot bound themselves differently
        session = FakeSession(
            [summary(str(i), i + 1) for i in range(10)],
            {str(i): detail() for i in range(10)},
        )
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(nTrials=3), trace_only()
        )
        assert len(result.trials) == 3


class TestResultsAreWritable:
    def test_a_sample_writes_metrics_without_a_suite(self):
        # _write_results read run["suiteId"] unconditionally, which is a KeyError
        # for a run that has none -- after every trace has been judged, which is
        # the most expensive moment to find out
        from hopsworks_agents.eval.metrics import _run_metrics

        trial = _as_trial("run1", 3, "t", "answer", None)
        trial.status = TrialStatus.PASSED
        trial.evaluator_results = [
            EvaluatorResult(
                evaluator_name="faithfulness",
                evaluator_type="llm_judge",
                score=1.0,
                passed=True,
            )
        ]
        rows = _run_metrics("run1", "", 3, [trial])
        assert any(r["metric_name"] == "pass_rate" for r in rows)
        assert all(r["suite_id"] == "" for r in rows)


class TestWhereTheNextRunStarts:
    """The watermark, which is what makes a schedule cover each trace once."""

    def test_it_reaches_the_end_of_the_window_when_all_of_it_was_graded(self):
        from hopsworks_agents.eval.sample_job import _graded_through

        end = ms(0)
        graded = [summary("a", 3), summary("b", 1)]
        assert _graded_through(graded, end) <= end

    def test_it_stops_at_the_last_graded_trace_when_the_ceiling_cut_in(self):
        # advancing to the window's end regardless would step over traces nobody
        # looked at, and nothing would ever report them
        from hopsworks_agents.eval.sample_job import _graded_through

        end = ms(0)
        graded = [summary("old", 10)]
        assert _graded_through(graded, end) < end - 3600_000

    def test_an_empty_window_still_advances(self):
        # nothing arrived, so there is nothing to come back for; holding the
        # watermark still would re-scan the same empty range forever
        from hopsworks_agents.eval.sample_job import _graded_through

        end = ms(0)
        assert _graded_through([], end) == end


def turn(
    trace_id: str, start_ns: int, question: str, answer: str, session_id: str = "s1"
) -> dict:
    """A trace as the session endpoint lists it: the root span with its messages."""
    return {
        "traceId": trace_id,
        "sessionId": session_id,
        "startTimeNs": start_ns,
        "messages": json.dumps(
            [
                {"role": "user", "content": question},
                {"role": "assistant", "content": answer},
            ]
        ),
    }


class TestTheConversationSoFar:
    """A judge asked about hallucination, relevance or frustration cannot answer.

    from one turn: "as I said, you own it" is a fact or an invention depending on
    the turn before. Live traffic has no task to carry that, but every trace
    names its session.
    """

    def graded(
        self,
        graded_start_ns: int,
        session_turns: list[dict],
        judge=None,
        session_id: str = "s1",
    ):
        judge = judge or StubJudge()
        # The turn times these tests care about are relative -- which turn came
        # before the graded one -- so they are written small and offset onto a
        # real clock here. The same field now decides the window as well, and a
        # trace that started 300ns after the epoch is not in anyone's last hour.
        base = int(summary("g", 1)["startTimeNs"])
        summary_row = {
            **summary("g", 1),
            "sessionId": session_id,
            "startTimeNs": base + graded_start_ns,
        }
        session = FakeSession(
            [summary_row],
            {"g": detail("Do I own any of them?", "Yes, two.")},
            {
                session_id: [
                    {**t, "startTimeNs": base + int(t["startTimeNs"])}
                    for t in session_turns
                ]
            },
        )
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), with_judge(judge)
        )
        return result, judge, session

    def test_earlier_turns_reach_the_judge_in_order(self):
        result, judge, _ = self.graded(
            300,
            [
                turn("t2", 200, "Aaron Mitchell, +1 (204) 452-6452", "Thanks, Aaron."),
                turn("t1", 100, "What albums do you have by Queen?", "Three: ..."),
            ],
        )
        prompt = judge.prompts[0]
        assert "<conversation>" in prompt
        # the block holds the whole exchange, oldest first, ending with the graded
        # turn; the prompt's own <question> and <agent_answer> repeat that turn
        block = prompt[prompt.index("<conversation>") : prompt.index("</conversation>")]
        assert block.index("What albums do you have by Queen?") < block.index(
            "Aaron Mitchell"
        )
        assert block.index("Aaron Mitchell") < block.index("Do I own any of them?")
        assert block.index("Do I own any of them?") < block.index("Yes, two.")
        assert result.trials[0].transcript.startswith("user: What albums")

    def test_the_future_is_not_shown(self):
        # a turn that came after the graded one is part of the same session, and
        # a judge shown it would be grading with knowledge the agent did not have
        _, judge, _ = self.graded(
            300,
            [
                turn("t1", 100, "Which AC/DC albums?", "Let There Be Rock, ..."),
                turn("t4", 400, "Order it then", "Recorded."),
            ],
        )
        assert "Which AC/DC albums?" in judge.prompts[0]
        assert "Order it then" not in judge.prompts[0]

    def test_a_first_turn_has_no_conversation_block(self):
        result, judge, _ = self.graded(100, [turn("later", 200, "x", "y")])
        assert "<conversation>" not in judge.prompts[0]
        assert result.trials[0].transcript == ""

    def test_a_trace_with_no_session_grades_alone(self):
        result, judge, session = self.graded(
            300, [turn("t1", 100, "x", "y")], session_id=""
        )
        assert "<conversation>" not in judge.prompts[0]
        assert session.session_fetches == 0

    def test_only_the_last_turns_of_a_long_conversation_are_shown(self):
        from hopsworks_agents.eval.sample_job import CONTEXT_TURNS

        turns = [
            turn(f"t{i}", i * 10, f"question {i}", f"answer {i}")
            for i in range(1, CONTEXT_TURNS + 6)
        ]
        _, judge, _ = self.graded(10_000, turns)
        assert (
            "question 1 " not in judge.prompts[0]
            and "question 1\n" not in judge.prompts[0]
        )
        assert f"question {CONTEXT_TURNS + 5}" in judge.prompts[0]
        assert f"question {6}" in judge.prompts[0]

    def test_a_session_is_fetched_once_per_run(self):
        judge = StubJudge()
        rows = [
            {
                **summary(f"g{i}", 1),
                "sessionId": "s1",
                "startTimeNs": int(summary(f"g{i}", 1)["startTimeNs"]) + i,
            }
            for i in range(3)
        ]
        session = FakeSession(
            rows,
            {f"g{i}": detail() for i in range(3)},
            {"s1": [turn("t1", 100, "x", "y")]},
        )
        _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), with_judge(judge)
        )
        assert session.session_fetches == 1
        assert len(judge.prompts) == 3

    def test_an_unreadable_session_grades_without_context_rather_than_not_at_all(self):
        class Broken(FakeSession):
            def get(self, url, params=None, timeout=None):
                if "/traces/sessions/" in url:
                    raise RuntimeError("down")
                return super().get(url, params, timeout)

        judge = StubJudge()
        row = {**summary("g", 1), "sessionId": "s1"}
        session = Broken([row], {"g": detail()}, {})
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), with_judge(judge)
        )
        assert len(result.trials) == 1
        assert "<conversation>" not in judge.prompts[0]

    def test_the_trial_records_its_session(self):
        result, _, _ = self.graded(300, [])
        assert result.trials[0].session_id == "s1"


class TestTheWindowIsTheServersJob:
    """The window is the server's job.

    The monitor used to ask for the newest 500 traces of the deployment and
    filter them to its window here, which above 500 in a window meant grading
    the newest slice and skipping everything older -- silently, since the list
    it compared against had already been cut server-side.
    """

    def test_the_listing_asks_for_the_window_oldest_first(self):
        session = FakeSession([summary("a", 1)], {"a": detail()})
        run = sample_run()
        _run_sample(FakeClient(), session, "https://h", 1, run, trace_only())
        assert session.listing_params["from"] == int(run["sampleFrom"])
        assert session.listing_params["to"] == int(run["sampleTo"])
        assert session.listing_params["order"] == "oldest"
        # and the server leaves the deployment's own eval runs out of both page and count
        assert session.listing_params["excludeEval"] == "true"
        # the ceiling, so the server returns at most what will be graded
        assert session.listing_params["limit"] == 500

    def test_a_ceiling_closes_the_window_where_the_run_stopped(self):
        # two of five graded: the next run must start at the newest of the two,
        # not at the end of this window, or the other three are never graded
        summaries = [summary(str(i), 5 - i) for i in range(5)]
        session = FakeSession(summaries, {str(i): detail() for i in range(5)}, count=5)
        result = _run_sample(
            FakeClient(),
            session,
            "https://h",
            1,
            sample_run(nTrials=2),
            trace_only(),
        )
        assert len(result.trials) == 2
        assert result.available == 5
        graded = sorted(s["startTimeNs"] / 1e6 for s in summaries[:2])
        # Past the newest graded, not at it: the backend opens the next window
        # at this value and filters start_time_ns strictly greater, and report
        # truncates it to an int -- so at the value itself a conversation whose
        # start carried a fraction of a millisecond was graded twice.
        assert result.processed_through_ms == math.floor(graded[-1]) + 1

    def test_a_random_sample_with_no_ceiling_grades_the_window(self):
        # Zero means "no ceiling" everywhere else here, and oldest_first reads it
        # that way. Random sliced the window to nothing, so a run row without a
        # ceiling graded no traffic at all and reported a quiet hour.
        summaries = [summary(str(i), i + 1) for i in range(3)]
        session = FakeSession(summaries, {str(i): detail() for i in range(3)}, count=3)
        result = _run_sample(
            FakeClient(),
            session,
            "https://h",
            1,
            sample_run(nTrials=0),
            trace_only(),
            strategy="random",
        )
        assert len(result.trials) == 3

    def test_graded_conversations_are_handed_over_as_they_go(self):
        # A monitor stopped half way through a long window used to leave nothing
        # at all, however many conversations it had already graded.
        import hopsworks_agents.eval.sample_job as sj

        summaries = [summary(str(i), i + 1) for i in range(5)]
        session = FakeSession(summaries, {str(i): detail() for i in range(5)}, count=5)
        batches = []
        saved, sj.WRITE_BATCH = sj.WRITE_BATCH, 2
        try:
            result = _run_sample(
                FakeClient(),
                session,
                "https://h",
                1,
                sample_run(),
                trace_only(),
                on_progress=batches.append,
            )
        finally:
            sj.WRITE_BATCH = saved

        assert len(result.trials) == 5
        assert [len(b) for b in batches] == [2, 2, 1]

    def test_a_window_whose_every_trace_is_unreadable_fails(self):
        # Not a quiet night: a broken read. Reported SUCCEEDED it moved the watermark over all
        # of them, so the traffic nobody graded was never offered again.
        class Unreadable(FakeSession):
            def get(self, url, params=None, timeout=None):
                if url.endswith("/traces") or "/traces/sessions/" in url:
                    return super().get(url, params, timeout)
                raise RuntimeError("the store is down")

        summaries = [summary("a", 2), summary("b", 1)]
        session = Unreadable(summaries, {}, count=2)
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )

        assert result.trials == []
        assert result.status == "FAILED"
        assert "could be read" in result.error_message
        assert result.processed_through_ms is None

    def test_an_empty_window_is_still_a_success(self):
        # an agent with no traffic in the window is a normal state, and failing it would page
        # someone about a quiet night
        session = FakeSession([], {}, count=0)
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )
        assert result.status == "SUCCEEDED"

    def test_a_window_graded_whole_leaves_its_end_alone(self):
        session = FakeSession(
            [summary("a", 2), summary("b", 1)], {"a": detail(), "b": detail()}, count=2
        )
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )
        assert result.processed_through_ms is None
        assert result.available == 2

    def test_a_random_sample_asks_the_server_to_spread_it(self):
        # The oldest N measures the start of the hour and calls it the hour. A
        # monitor that cannot grade its window whole can ask for a spread
        # instead -- and then it does not come back for the rest, so the window
        # end stands.
        summaries = [summary(str(i), 5 - i) for i in range(5)]
        session = FakeSession(summaries, {str(i): detail() for i in range(5)}, count=5)
        result = _run_sample(
            FakeClient(),
            session,
            "https://h",
            1,
            sample_run(nTrials=2),
            trace_only(),
            strategy="random",
        )
        assert session.listing_params["order"] == "random"
        assert len(result.trials) == 2
        assert result.available == 5
        # the window was sampled, not partly graded: nothing is deferred
        assert result.processed_through_ms is None

    def test_every_evaluator_of_an_execution_draws_the_same_sample(self):
        # Each drawing its own makes every rate a fair sample of the hour and no
        # two of them about the same conversations, so "this judge passed it and
        # that one failed it" stops being a question anyone can ask.
        session = FakeSession([summary("a", 1)], {"a": detail()}, count=5)
        run = sample_run(nTrials=2)
        _run_sample(
            FakeClient(),
            session,
            "https://h",
            1,
            run,
            trace_only(),
            strategy="random",
        )
        assert session.listing_params["seed"] == int(run["sampleFrom"])

    def test_an_independent_draw_asks_for_no_seed(self):
        session = FakeSession([summary("a", 1)], {"a": detail()}, count=5)
        _run_sample(
            FakeClient(),
            session,
            "https://h",
            1,
            sample_run(nTrials=2),
            trace_only(),
            strategy="random",
            shared=False,
        )
        assert "seed" not in session.listing_params

    def test_the_oldest_strategy_needs_no_seed_to_agree(self):
        # ordering by time is already the same answer for every evaluator
        session = FakeSession([summary("a", 1)], {"a": detail()}, count=5)
        _run_sample(FakeClient(), session, "https://h", 1, sample_run(), trace_only())
        assert "seed" not in session.listing_params

    def test_a_backend_that_ignores_the_window_is_still_filtered_here(self):
        # an older cluster answers with the newest page of everything and no
        # count; grading that as if it were the window would be wrong, not slow
        session = FakeSession(
            [summary("recent", 1), summary("ancient", 500)],
            {"recent": detail(), "ancient": detail()},
        )
        result = _run_sample(
            FakeClient(), session, "https://h", 1, sample_run(), trace_only()
        )
        assert {t.task_id for t in result.trials} == {"recent"}
        assert result.available == 1
        assert result.processed_through_ms is None
