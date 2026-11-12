"""A trial may run as a real person, and must not write as one.

A task promoted from a production turn replays with the subject that turn ran
as -- the only way a memory regression can fail for the reason the original
did. Reading that person's memory is the point. Writing it is not: a regression
suite run nightly would otherwise rewrite a customer's stored profile with
whatever the agent concluded during the test, and file every replayed question
in their conversation history.
"""

from __future__ import annotations

from hopsworks_agents.protocol.context import HandlerContext
from hopsworks_agents.protocol.models import ChatRequest


class FakeMemory:
    """Durable state keyed by (scope, owner, key), and the rows a restamp touched."""

    def __init__(self, facts=None):
        self.facts = dict(facts or {})
        self.restamped = []

    def get_state(self, scope, owner, key):
        return self.facts.get((scope, owner, key))

    def list_state(self, scope, owner):
        return [
            {"key": k, "value": v}
            for (s, o, k), v in self.facts.items()
            if s == scope and o == owner
        ]

    def set_state(self, scope, owner, key, value, source_ref=None):
        self.facts[(scope, owner, key)] = value

    def delete_state(self, scope, owner, key):
        return 1 if self.facts.pop((scope, owner, key), None) is not None else 0

    def rebind_turn_subject(self, conversation_id, turn_id, subject):
        self.restamped.append(subject)


def ctx(
    memory, *, subject="marcus@example.com", trial=True, conversation_id="run/task/1/0"
):
    request = ChatRequest.model_validate(
        {
            "conversation_id": conversation_id,
            "message": {"role": "user", "content": [{"type": "text", "text": "hi"}]},
            **({"subject": subject} if subject else {}),
        }
    )
    context = HandlerContext(
        request=request,
        memory=memory,
        framework="custom",
        response_id="r1",
        turn_id="t1",
        subject=subject,
    )
    context._eval_labelled = trial
    return context


PROFILE = {("user", "marcus@example.com", "risk_attitude"): "aggressive"}


class TestATrialReadsThePerson:
    def test_their_facts_are_visible(self):
        assert ctx(FakeMemory(PROFILE)).recall("risk_attitude") == "aggressive"

    def test_state_lists_them_too(self):
        assert ctx(FakeMemory(PROFILE)).state()["risk_attitude"] == "aggressive"


class TestATrialDoesNotWriteThePerson:
    def test_a_remember_lands_under_the_trial(self):
        memory = FakeMemory(PROFILE)
        trial = ctx(memory)
        trial.remember("risk_attitude", "conservative")
        assert (
            memory.facts[("user", "marcus@example.com", "risk_attitude")]
            == "aggressive"
        )
        assert memory.facts[("user", "run/task/1/0", "risk_attitude")] == "conservative"

    def test_and_the_trial_reads_its_own_write_back(self):
        memory = FakeMemory(PROFILE)
        trial = ctx(memory)
        trial.remember("risk_attitude", "conservative")
        assert trial.recall("risk_attitude") == "conservative"
        assert trial.state()["risk_attitude"] == "conservative"

    def test_a_forget_cannot_erase_their_fact(self):
        memory = FakeMemory(PROFILE)
        assert ctx(memory).forget("risk_attitude") == 0
        assert ("user", "marcus@example.com", "risk_attitude") in memory.facts

    def test_messages_are_filed_under_the_trial(self):
        assert ctx(FakeMemory())._stamp_subject == "run/task/1/0"

    def test_a_rebind_in_a_trial_does_not_restamp_rows_as_the_person(self):
        memory = FakeMemory()
        trial = ctx(memory, subject=None)
        trial.rebind_subject("dana@example.com")
        assert memory.restamped == []
        trial.remember("risk_attitude", "balanced")
        assert ("user", "dana@example.com", "risk_attitude") not in memory.facts


class TestAPersonsOwnTurnIsUnchanged:
    def test_writes_go_to_them(self):
        memory = FakeMemory()
        turn = ctx(memory, trial=False)
        turn.remember("risk_attitude", "balanced")
        assert (
            memory.facts[("user", "marcus@example.com", "risk_attitude")] == "balanced"
        )
        assert turn._stamp_subject == "marcus@example.com"

    def test_session_scope_is_the_conversation_either_way(self):
        memory = FakeMemory()
        ctx(memory).remember("ticker", "MSFT", scope="session")
        assert memory.facts[("session", "run/task/1/0", "ticker")] == "MSFT"


class TestTheTurnRecordsWhatAReplayNeeds:
    """The span says who the turn ran as and what the client sent with it.

    A promoted task is built from the trace, and without these a regression of a
    memory or personalisation failure replays as an anonymous user who never had
    the memory in question -- and passes.
    """

    @staticmethod
    def _spans(body, handler=None):
        import pytest

        pytest.importorskip("opentelemetry.sdk")
        from fastapi.testclient import TestClient
        from hopsworks_agents.protocol import AgentApp
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        app = AgentApp(name="Replayable agent", tracing=False)
        app.tracer_provider = provider

        async def chat(request, context):
            if handler:
                handler(context)
            return "ok"

        app.chat(chat)
        TestClient(app).post("/v1/chat", json=body)
        return dict(exporter.get_finished_spans()[-1].attributes)

    @staticmethod
    def _body(**extra):
        return {
            "conversation_id": "c1",
            "message": {"role": "user", "content": [{"type": "text", "text": "hi"}]},
            **extra,
        }

    def test_a_client_subject_and_its_source(self):
        attrs = self._spans(self._body(subject="marcus@example.com"))
        assert attrs["user.id"] == "marcus@example.com"
        assert attrs["hopsworks.subject.source"] == "client"

    def test_a_subject_the_agent_bound_mid_turn(self):
        attrs = self._spans(
            self._body(), handler=lambda c: c.rebind_subject("dana@example.com")
        )
        assert attrs["user.id"] == "dana@example.com"
        assert attrs["hopsworks.subject.source"] == "app"

    def test_no_subject_is_no_subject(self):
        attrs = self._spans(self._body())
        assert "user.id" not in attrs
        assert "hopsworks.subject.source" not in attrs

    def test_the_request_context_as_json(self):
        import json

        attrs = self._spans(self._body(context={"plan": "pro", "seats": 3}))
        assert json.loads(attrs["hopsworks.request.context"]) == {
            "plan": "pro",
            "seats": 3,
        }

    def test_a_context_too_large_to_keep_whole_is_left_off(self):
        attrs = self._spans(self._body(context={"blob": "x" * 20_000}))
        assert "hopsworks.request.context" not in attrs
