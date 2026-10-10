"""Running a task that is a conversation rather than a question.

The failure these guard against is specific: every turn can be individually
reasonable and the conversation still be wrong, and the runner used to send only
the last turn — which produced a pass rate for an exchange that never happened.
"""

import json

import pytest
from hopsworks_agents.eval.judge_config import parse_judge_config, render_prompt
from hopsworks_agents.eval.models import Suite, Task, TrialStatus
from hopsworks_agents.eval.runner import AgentResponse, RunnerConfig, run_suite


CAPABLE = {"capabilities": {"trace_correlation": True, "eval_mode": True}}


class ScriptedAgent:
    """Replies in order, recording what it was asked and how."""

    def __init__(self, replies, *, error_on=None):
        self._replies = list(replies)
        self._error_on = error_on
        self.calls: list[dict] = []

    def manifest(self):
        return CAPABLE

    def call(self, prompt, *, traceparent, baggage, timeout_s, conversation_id=None):
        self.calls.append({"prompt": prompt, "conversation_id": conversation_id})
        index = len(self.calls) - 1
        if self._error_on == index:
            return AgentResponse(text="", error="upstream refused", latency_ms=1.0)
        reply = self._replies[index] if index < len(self._replies) else "ok"
        return AgentResponse(
            text=reply, trace_id=traceparent.split("-")[1], latency_ms=1.0
        )

    def fetch_trace(self, trace_id):
        return None


def conversation(*turns, task_id="c1"):
    return Task(
        task_id=task_id,
        input_messages=json.dumps([{"role": "user", "content": t} for t in turns]),
        task_type="multi_turn",
    )


def run(client, task):
    return run_suite(
        client,
        Suite(suite_id="s1", tasks=[task]),
        run_id="run-1",
        deployment_id=7,
        evaluators=[],
        config=RunnerConfig(readiness_timeout_s=0.01, readiness_poll_s=0.001),
        sleep=lambda _: None,
    )


class TestTheTurnsAreSent:
    def test_every_turn_is_sent_in_order(self):
        client = ScriptedAgent(["who are you?", "thanks", "done"])

        run(client, conversation("hello", "Aaron Mitchell", "order it"))

        assert [c["prompt"] for c in client.calls] == [
            "hello",
            "Aaron Mitchell",
            "order it",
        ]

    def test_the_turns_share_one_conversation(self):
        # the agent's own memory has to see them as one exchange, or a suite
        # testing whether it remembers what it was told tests nothing
        client = ScriptedAgent(["a", "b"])

        run(client, conversation("first", "second"))

        ids = {c["conversation_id"] for c in client.calls}
        assert len(ids) == 1
        assert ids != {None}

    def test_a_single_turn_task_is_sent_exactly_as_before(self):
        # no conversation id, so a client written against the old signature
        # keeps working
        client = ScriptedAgent(["hi"])

        run(
            client,
            Task(task_id="t1", input_messages='[{"role":"user","content":"hi"}]'),
        )

        assert client.calls[0]["conversation_id"] is None


class TestWhatTheTrialRecords:
    def test_the_transcript_is_both_sides_in_order(self):
        client = ScriptedAgent(["who is this?", "you bought it in March"])

        result = run(client, conversation("did I buy it?", "Aaron Mitchell"))
        trial = result.trials[0]

        assert trial.transcript.splitlines()[0] == "user: did I buy it?"
        assert "agent: who is this?" in trial.transcript
        assert trial.transcript.index("did I buy it?") < trial.transcript.index(
            "Aaron Mitchell"
        )

    def test_the_last_answer_is_the_final_output(self):
        client = ScriptedAgent(["asking", "the answer"])

        trial = run(client, conversation("one", "two")).trials[0]

        assert trial.final_output == "the answer"

    def test_the_session_is_recorded_for_a_conversation_only(self):
        many = run(ScriptedAgent(["a", "b"]), conversation("one", "two")).trials[0]
        one = run(
            ScriptedAgent(["a"]),
            Task(task_id="t1", input_messages='[{"role":"user","content":"hi"}]'),
        ).trials[0]

        assert many.session_id
        # a single turn is a conversation of one, and says nothing the trace id
        # does not already say
        assert one.session_id == ""
        assert one.transcript == ""


class TestWhenATurnFails:
    def test_the_rest_of_the_script_is_not_sent(self):
        # sending turn 3 would answer a question the agent never asked, and
        # grade a transcript with a hole in it as though it were whole
        client = ScriptedAgent(["fine", "", "never reached"], error_on=1)

        run(client, conversation("one", "two", "three"))

        assert [c["prompt"] for c in client.calls] == ["one", "two"]

    def test_the_trial_fails_and_keeps_what_was_said(self):
        client = ScriptedAgent(["fine", ""], error_on=1)

        trial = run(client, conversation("one", "two", "three")).trials[0]

        assert trial.status is TrialStatus.FAILED
        assert "upstream refused" in trial.error_message
        assert "user: one" in trial.transcript


class TestTheJudgeSeesIt:
    def test_the_transcript_reaches_the_prompt(self):
        prompt = render_prompt(
            parse_judge_config(
                {
                    "type": "llm_judge",
                    "criteria": {"resolved": {"weight": 1, "description": "d"}},
                }
            ),
            question="q",
            answer="a",
            transcript="user: hi\n\nagent: hello",
        )

        assert "<conversation>" in prompt
        assert "agent: hello" in prompt

    def test_a_single_turn_prompt_has_no_conversation_block(self):
        prompt = render_prompt(
            parse_judge_config(
                {
                    "type": "llm_judge",
                    "criteria": {"ok": {"weight": 1, "description": "d"}},
                }
            ),
            question="q",
            answer="a",
        )

        assert "<conversation>" not in prompt


class TestTheTaskItself:
    def test_turns_reads_the_user_side_in_order(self):
        assert conversation("a", "b", "c").turns() == ["a", "b", "c"]

    def test_assistant_messages_are_not_part_of_the_script(self):
        # [user, assistant, user] is ambiguous between a two-turn script and a
        # one-turn script with seeded history; only the user side is the script
        task = Task(
            task_id="t1",
            input_messages=json.dumps(
                [
                    {"role": "user", "content": "one"},
                    {"role": "assistant", "content": "reply"},
                    {"role": "user", "content": "two"},
                ]
            ),
            task_type="multi_turn",
        )

        assert task.turns() == ["one", "two"]

    def test_prompt_refuses_a_conversation_rather_than_narrowing_it(self):
        with pytest.raises(ValueError, match="3 user turns"):
            _ = conversation("a", "b", "c").prompt

    def test_asked_gives_a_judge_every_turn(self):
        assert conversation("a", "b").asked == "1. a\n2. b"

    def test_asked_leaves_a_single_turn_alone(self):
        assert (
            Task(task_id="t1", input_messages='[{"role":"user","content":"hi"}]').asked
            == "hi"
        )
