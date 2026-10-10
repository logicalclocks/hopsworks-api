"""Which conversation was with whom.

A support agent is told who it is talking to *during* the conversation — it
asks for a customer key and the answer arrives on turn two. By then turn one's
spans have been exported and are immutable, so no span attribute can ever say
who that turn was with. This index is read at query time instead, which is what
lets one row attribute a whole conversation retroactively.

What it buys: a developer holding a customer key can find that customer's
conversations. That is the entire purpose. Nothing reads it back into the
agent, and writing it does not change what the agent can remember or recall —
see :meth:`~hopsworks_agents.protocol.context.HandlerContext.record_subject`.

The table is a Hopsworks feature group, created by the platform when the
deployment starts. This module only writes rows: a deployment whose project
predates the feature group logs once and carries on.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone

from .memory import FEATURE_GROUP_VERSION


log = logging.getLogger(__name__)

#: The feature group the platform provisions. Name must match
#: TracingStoreFacade.AGENT_CONVERSATION_SUBJECTS.
SUBJECTS_FG = "agent_conversation_subjects"

#: The table that feature group becomes online, which carries its version. A
#: feature group named X at version N is a table named X_N -- writing to the
#: bare name finds nothing, which is what "does not exist in this project" meant
#: while the feature group was sitting there in the UI.
SUBJECTS_TABLE = f"{SUBJECTS_FG}_{FEATURE_GROUP_VERSION}"

#: Where a subject came from. Stored beside it because a subject is a claim,
#: not a verified identity, and a reader shown one without the other is being
#: told a model's guess is a fact.
SOURCE_CLIENT = "client"
SOURCE_APP = "app"
SOURCE_MODEL = "model"
SOURCE_CONVERSATION = "conversation"

_SOURCES = (SOURCE_CLIENT, SOURCE_APP, SOURCE_MODEL, SOURCE_CONVERSATION)


class ConversationSubjectIndex:
    """Records who a conversation is with, best-effort.

    Every failure here is swallowed and logged. The index is a debugging aid;
    a database that is down, a table that was never created, or a deployment id
    that is not numeric must never cost a user their reply.
    """

    def __init__(self, url: str | None = None, deployment_id: str | None = None):
        self._url = url
        self._deployment_id = deployment_id
        self._engine = None
        self._table = None
        self._lock = threading.Lock()
        # Retryable, not sticky. The table is created by the platform during the
        # same deployment start that brings this pod up, so a first turn can
        # genuinely arrive before it exists -- and latching meant the process
        # never looked again, leaving traces unattributed until someone
        # restarted it for reasons they could not have guessed. Backoff so a
        # table that really is absent is not asked about once per turn.
        self._retry_after = 0.0
        self._backoff = 1.0

    def record(self, conversation_id: str, subject: str, source: str) -> bool:
        """Attribute a conversation to a subject. Returns whether a row landed.

        Idempotent by key: the last claim wins. No history is kept here because
        the tool call that made the claim is already an event on the trace, so
        a conversation that named three different customers shows that where
        someone debugging it is already looking.
        """
        if not conversation_id or not subject:
            return False
        if source not in _SOURCES:
            log.warning(
                "Unknown subject source %r; recording as %r", source, SOURCE_APP
            )
            source = SOURCE_APP
        table = self._ensure_table()
        if table is None:
            return False
        values = {
            "subject": subject,
            "subject_source": source,
            "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        # Update-then-insert rather than the MySQL ON DUPLICATE KEY form, to
        # match how the memory store writes and to keep this runnable against
        # any database a test points it at.
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    table.update()
                    .where(table.c.deployment_id == self._numeric_deployment_id)
                    .where(table.c.conversation_id == conversation_id)
                    .values(**values)
                )
                if not result.rowcount:
                    conn.execute(
                        table.insert().values(
                            deployment_id=self._numeric_deployment_id,
                            conversation_id=conversation_id,
                            **values,
                        )
                    )
            return True
        except Exception:  # noqa: BLE001 — identity must never break a turn
            log.exception(
                "Could not record the subject of conversation %s", conversation_id
            )
            return False

    def _ensure_table(self):
        if self._table is not None:
            return self._table
        if time.monotonic() < self._retry_after:
            return None
        with self._lock:
            if self._table is not None:
                return self._table
            if time.monotonic() < self._retry_after:
                return None
            try:
                from sqlalchemy import (
                    BigInteger,
                    Column,
                    DateTime,
                    MetaData,
                    String,
                    Table,
                    create_engine,
                )

                from .memory import deployment_mysql_url

                deployment = self._deployment_id or os.environ.get("DEPLOYMENT_ID")
                if deployment is None:
                    raise RuntimeError(
                        "DEPLOYMENT_ID is not set, so a conversation cannot be "
                        "attributed to this agent."
                    )
                # bigint on the platform side, to join otel_spans without a
                # cast. A deployment id that is not a number would be silently
                # coerced by MySQL, so it is rejected here instead.
                self._numeric_deployment_id = int(str(deployment))

                url = self._url or deployment_mysql_url()
                self._engine = create_engine(url, pool_pre_ping=True)
                # Asked once, here, rather than discovered on every write: the
                # platform creates this table when the deployment starts, so a
                # project that predates it will never have one, and a failure
                # per conversation would bury the single warning that matters.
                from sqlalchemy import inspect as sqla_inspect

                if not sqla_inspect(self._engine).has_table(SUBJECTS_TABLE):
                    raise RuntimeError(
                        f"{SUBJECTS_TABLE} does not exist in this project"
                    )
                self._table = Table(
                    SUBJECTS_TABLE,
                    MetaData(),
                    Column("deployment_id", BigInteger, primary_key=True),
                    Column("conversation_id", String(255), primary_key=True),
                    Column("subject", String(255), nullable=True),
                    Column("subject_source", String(16), nullable=True),
                    Column("updated_at", DateTime, nullable=False),
                )
                return self._table
            except Exception:  # noqa: BLE001
                self._engine = None
                self._retry_after = time.monotonic() + self._backoff
                self._backoff = min(self._backoff * 2, 300.0)
                log.warning(
                    "Conversation subjects are not being recorded (%s is "
                    "unavailable); retrying in %.0fs, traces show no subject "
                    "until then",
                    SUBJECTS_TABLE,
                    self._backoff,
                    exc_info=True,
                )
                return None
