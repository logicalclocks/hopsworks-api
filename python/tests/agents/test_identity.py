"""Recording who a conversation was with.

The point of the index is that a developer holding a customer key can find that
customer's conversations. These tests cover the two things that would silently
break it: not recording, and recording under an authority it was not given.
"""

import pytest
from fastapi.testclient import TestClient
from hopsworks_agents.protocol import AgentApp, conventions
from hopsworks_agents.protocol.context import HandlerContext
from hopsworks_agents.protocol.identity import (
    SUBJECTS_TABLE,
    ConversationSubjectIndex,
)
from hopsworks_agents.protocol.models import ChatRequest


class FakeIndex:
    def __init__(self, ok=True):
        self.rows = []
        self.ok = ok

    def record(self, conversation_id, subject, source):
        self.rows.append((conversation_id, subject, source))
        return self.ok


def make_request(text="hi", conversation_id="c1", subject=None):
    body = {
        "conversation_id": conversation_id,
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }
    if subject is not None:
        body["subject"] = subject
    return body


def context(subjects, subject=None, conversation_id="c1"):
    request = ChatRequest.model_validate(
        make_request(conversation_id=conversation_id, subject=subject)
    )
    return HandlerContext(
        request=request,
        memory=None,
        framework="custom",
        response_id="r1",
        turn_id="t1",
        subject=subject,
        subjects=subjects,
    )


class TestRecordSubject:
    def test_records_the_conversation_and_its_source(self):
        index = FakeIndex()
        ctx = context(index)

        assert ctx.record_subject("cust-1", source="model") is True
        assert index.rows == [("c1", "cust-1", "model")]

    def test_recording_does_not_change_what_memory_the_turn_can_read(self):
        index = FakeIndex()
        ctx = context(index)

        ctx.record_subject("cust-1", source="model")

        # the whole reason the tool calls this and not rebind_subject: subject
        # keys durable memory, so a model that could set it could read another
        # customer's facts
        assert ctx.subject == "c1"
        assert ctx.subject_source == "conversation"
        assert ctx.has_subject is False

    def test_rebinding_also_records_it(self):
        index = FakeIndex()
        ctx = context(index)

        ctx.rebind_subject("cust-1")

        # one writer for both, or the index and the memory rows end up
        # disagreeing about who this conversation is with
        assert ctx.subject == "cust-1"
        assert index.rows == [("c1", "cust-1", "app")]

    def test_no_index_configured_is_not_an_error(self):
        ctx = context(None)

        assert ctx.record_subject("cust-1") is False
        # and rebinding still does its own job
        ctx.rebind_subject("cust-1")
        assert ctx.subject == "cust-1"

    def test_empty_subject_records_nothing(self):
        index = FakeIndex()
        ctx = context(index)

        assert ctx.record_subject("") is False
        assert index.rows == []


class TestIdentifyTool:
    def _ctx(self, index):
        from hopsworks_agents.protocol.autoevents import current_context

        ctx = context(index)
        token = current_context.set(ctx)
        return ctx, token

    def test_identify_records_the_claim_as_a_model_claim(self):
        from hopsworks_agents.protocol.autoevents import current_context
        from hopsworks_agents.protocol.tools import identify

        index = FakeIndex()
        _, token = self._ctx(index)
        try:
            result = identify("cust-1")
        finally:
            current_context.reset(token)

        # source 'model', never 'app' or 'client': an audit view that cannot
        # tell a model's claim from a verified one is worse than no column
        assert index.rows == [("c1", "cust-1", "model")]
        assert "cust-1" in result

    def test_identify_trims_and_refuses_an_empty_claim(self):
        from hopsworks_agents.protocol.autoevents import current_context
        from hopsworks_agents.protocol.tools import identify

        index = FakeIndex()
        _, token = self._ctx(index)
        try:
            identify("  cust-2  ")
            blank = identify("   ")
        finally:
            current_context.reset(token)

        assert index.rows == [("c1", "cust-2", "model")]
        assert "nothing was recorded" in blank.lower()

    def test_identify_outside_a_turn_tells_the_model_not_to_retry(self):
        from hopsworks_agents.protocol.tools import identify

        result = identify("cust-1")

        assert "do not retry" in result.lower()

    def test_identify_reports_a_failed_write_rather_than_claiming_success(self):
        from hopsworks_agents.protocol.autoevents import current_context
        from hopsworks_agents.protocol.tools import identify

        index = FakeIndex(ok=False)
        _, token = self._ctx(index)
        try:
            result = identify("cust-1")
        finally:
            current_context.reset(token)

        assert "could not be recorded" in result.lower()

    def test_identify_is_not_registered_with_the_memory_tools(self):
        from hopsworks_agents.protocol.tools import MEMORY_TOOLS, identity_tools

        # opt-in: what it writes is what an audit surface shows as the person on
        # the other end, so an app asks for it rather than receiving it
        assert identify_name_not_in(MEMORY_TOOLS)
        assert [fn.__name__ for fn in identity_tools("plain")] == ["identify"]


def identify_name_not_in(tools):
    return "identify" not in [fn.__name__ for fn in tools]


class TestUserIdSpanAttribute:
    def test_asserted_subject_is_stamped_on_the_turn_span(self):
        app = AgentApp()

        @app.chat
        async def chat(request):
            return "ok"

        ctx = context(None, subject="cust-1")
        attributes = app._turn_span_attributes(ctx)

        # the sidecar reads user.id into otel_spans.user_id, so a client that
        # already knows who it is needs no index row at all
        assert attributes[conventions.USER_ID] == "cust-1"

    def test_the_conversation_fallback_is_not_published_as_a_user(self):
        app = AgentApp()

        @app.chat
        async def chat(request):
            return "ok"

        ctx = context(None)
        attributes = app._turn_span_attributes(ctx)

        # ctx.subject is the conversation id here. Stamping it would publish a
        # conversation id as somebody's identity.
        assert conventions.USER_ID not in attributes


class TestTheTableItAddresses:
    def test_the_table_carries_the_feature_group_version(self):
        # A feature group named X at version N is an online table named X_N.
        # Writing to the bare name found nothing, and reported "does not exist
        # in this project" while the feature group sat there in the UI.
        from hopsworks_agents.protocol.identity import SUBJECTS_FG
        from hopsworks_agents.protocol.memory import FEATURE_GROUP_VERSION

        assert f"{SUBJECTS_FG}_{FEATURE_GROUP_VERSION}" == SUBJECTS_TABLE
        assert SUBJECTS_TABLE != SUBJECTS_FG

    def test_it_is_versioned_the_same_way_the_memory_tables_are(self):
        # one convention, so a reader who has seen ITEMS_TABLE knows this one
        from hopsworks_agents.protocol.memory import ITEMS_TABLE, MESSAGES_FG

        assert ITEMS_TABLE.removeprefix(MESSAGES_FG) == SUBJECTS_TABLE.removeprefix(
            "agent_conversation_subjects"
        )


class TestIndexIsBestEffort:
    def test_a_missing_table_is_not_asked_about_every_turn(self, caplog):
        index = ConversationSubjectIndex(url="sqlite:///:memory:", deployment_id="1")

        assert index.record("c1", "cust-1", "model") is False
        assert index.record("c2", "cust-2", "model") is False

        # backed off rather than asked per conversation, which would bury the
        # warning it is trying to raise
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1

    def test_a_table_created_after_the_process_started_is_picked_up(self, tmp_path):
        # what actually happened: the platform creates the table during the same
        # deployment start that brings the pod up, so the first turn can arrive
        # before it exists. Latching meant the process never looked again.
        from sqlalchemy import (
            BigInteger,
            Column,
            DateTime,
            MetaData,
            String,
            Table,
            create_engine,
        )

        url = f"sqlite:///{tmp_path}/late.db"
        index = ConversationSubjectIndex(url=url, deployment_id="1")
        assert index.record("c1", "cust-1", "model") is False

        metadata = MetaData()
        Table(
            SUBJECTS_TABLE,
            metadata,
            Column("deployment_id", BigInteger, primary_key=True),
            Column("conversation_id", String(255), primary_key=True),
            Column("subject", String(255)),
            Column("subject_source", String(16)),
            Column("updated_at", DateTime, nullable=False),
        )
        metadata.create_all(create_engine(url))

        # The wait has to be a wait, not a latch: this is what the failure was.
        import time as _time

        assert index._retry_after - _time.monotonic() <= 60

        index._retry_after = 0.0  # that wait, elapsed
        assert index.record("c1", "cust-1", "model") is True

    def test_a_non_numeric_deployment_id_is_refused_not_coerced(self):
        index = ConversationSubjectIndex(
            url="sqlite:///:memory:", deployment_id="not-a-number"
        )

        # the column is bigint so it joins otel_spans without a cast; MySQL
        # would quietly turn 'not-a-number' into 0 and pool every such
        # deployment's conversations onto one id
        assert index.record("c1", "cust-1", "model") is False

    @pytest.mark.parametrize("blank", ["", None])
    def test_nothing_is_written_without_both_ids(self, blank):
        index = ConversationSubjectIndex(url="sqlite:///:memory:", deployment_id="1")

        assert index.record(blank, "cust-1", "model") is False
        assert index.record("c1", blank, "model") is False


class TestTheRowItself:
    def _index(self, tmp_path):
        """An index over a real table, standing in for the platform's."""
        from sqlalchemy import (
            BigInteger,
            Column,
            DateTime,
            MetaData,
            String,
            Table,
            create_engine,
        )

        url = f"sqlite:///{tmp_path}/subjects.db"
        metadata = MetaData()
        Table(
            SUBJECTS_TABLE,
            metadata,
            Column("deployment_id", BigInteger, primary_key=True),
            Column("conversation_id", String(255), primary_key=True),
            Column("subject", String(255)),
            Column("subject_source", String(16)),
            Column("updated_at", DateTime, nullable=False),
        )
        metadata.create_all(create_engine(url))
        return ConversationSubjectIndex(url=url, deployment_id="42"), url

    def _rows(self, url):
        from sqlalchemy import create_engine, text

        with create_engine(url).connect() as conn:
            return conn.execute(
                text(
                    "SELECT deployment_id, conversation_id, subject, "
                    f"subject_source FROM {SUBJECTS_TABLE}"
                )
            ).fetchall()

    def test_a_claim_lands_as_one_row_keyed_by_the_conversation(self, tmp_path):
        index, url = self._index(tmp_path)

        assert index.record("conv-1", "cust-1", "model") is True

        assert self._rows(url) == [(42, "conv-1", "cust-1", "model")]

    def test_the_last_claim_wins_without_adding_a_row(self, tmp_path):
        index, url = self._index(tmp_path)

        index.record("conv-1", "cust-1", "model")
        index.record("conv-1", "cust-2", "app")

        # one row per conversation. The claim history is already on the trace
        # as tool-call events, so keeping versions here would duplicate it.
        assert self._rows(url) == [(42, "conv-1", "cust-2", "app")]

    def test_conversations_of_different_deployments_do_not_collide(self, tmp_path):
        index, url = self._index(tmp_path)
        other = ConversationSubjectIndex(url=url, deployment_id="43")

        index.record("conv-1", "cust-1", "model")
        other.record("conv-1", "someone-else", "model")

        assert sorted(self._rows(url)) == [
            (42, "conv-1", "cust-1", "model"),
            (43, "conv-1", "someone-else", "model"),
        ]

    def test_an_unknown_source_is_recorded_as_app_not_as_itself(self, tmp_path):
        index, url = self._index(tmp_path)

        index.record("conv-1", "cust-1", "verified-by-magic")

        # the column is what a UI trusts to say how strong a claim is; an
        # unrecognised value there would be shown as if it meant something
        assert self._rows(url)[0][3] == "app"


class TestEndToEnd:
    def test_a_turn_carries_the_index_into_the_handler(self):
        app = AgentApp()
        index = FakeIndex()
        app._subjects = index
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["recorded"] = ctx.record_subject("cust-9", source="app")
            return "ok"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request(conversation_id="conv-7"))

        assert seen["recorded"] is True
        assert index.rows == [("conv-7", "cust-9", "app")]
