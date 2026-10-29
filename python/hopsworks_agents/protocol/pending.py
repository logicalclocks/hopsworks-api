"""The questions a paused turn waits on, kept where the resume will find them.

``ctx.interrupt`` pauses a turn; the person's answer arrives later as a new
request, which any replica of the agent may receive. What the turn waits on --
the request it replays and the questions it asked -- is therefore kept by the
memory store when the agent has one that is shared (:class:`ManagedMemoryService`
writes it to its own table), and in the process otherwise.

A record moves through three states. It is written when the turn pauses. A
resume *claims* it before replaying, so two replicas handed the same answer do
not both run the action it approves. Then the replay either completes, and the
record is deleted, or fails, and the claim is released so the person can answer
again. A claim older than :data:`CLAIM_LEASE` is taken to belong to a replica
that died mid-turn, and the record can be claimed again.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable


#: How long a resume holds its claim. A turn that runs longer than this can be
#: resumed a second time, so it is generous; a replica that dies mid-turn leaves
#: its question unanswerable for this long, which is the price of that.
CLAIM_LEASE = timedelta(minutes=15)

#: How long a record whose questions never expire is kept. A conversation that
#: is never answered and never continued would otherwise hold it for ever.
RETENTION = timedelta(days=30)

#: How long a record outlives its last question's expiry: long enough to tell a
#: late answer that the question expired, rather than that nothing was asked.
EXPIRED_GRACE = timedelta(days=1)

#: The process store's bound: a pod that pauses many conversations nobody
#: answers forgets the oldest rather than growing without limit.
PROCESS_LIMIT = 10_000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _claimed(claimed_at: datetime | None, now: datetime) -> bool:
    return claimed_at is not None and claimed_at > now - CLAIM_LEASE


@dataclass
class _Pending:
    """One paused conversation: its record, the version a claim must match, and whether a resume holds it."""

    record: str
    version: int
    claimed: bool


class _PendingQuestions(ABC):
    """Where paused conversations are kept. Every method may raise when the store is unreachable."""

    @abstractmethod
    def _get(self, conversation_id: str) -> _Pending | None:
        """The conversation's record, or None when it is not paused."""

    @abstractmethod
    def _put(self, conversation_id: str, record: str, expires_at: datetime) -> None:
        """Record what the conversation now waits on, unclaimed, replacing what it waited on before."""

    @abstractmethod
    def _claim(self, conversation_id: str, version: int) -> bool:
        """Take the record for one resume; False when another resume took it first."""

    @abstractmethod
    def _release(self, conversation_id: str) -> None:
        """Give a claim back: the resume did not finish, and the question is open again."""

    @abstractmethod
    def _clear(self, conversation_id: str) -> None:
        """Forget the record: its questions are answered, or the conversation moved on."""


class _ProcessPending(_PendingQuestions):
    """In this process only: a restarted pod, or another replica, does not know about it."""

    def __init__(self, limit: int = PROCESS_LIMIT):
        self._limit = limit
        self._rows: OrderedDict[str, dict] = OrderedDict()
        self._lock = threading.Lock()

    def _get(self, conversation_id: str) -> _Pending | None:
        now = _utcnow()
        with self._lock:
            row = self._rows.get(conversation_id)
            if row is None:
                return None
            if row["expires_at"] < now:
                del self._rows[conversation_id]
                return None
            return _Pending(
                row["record"], row["version"], _claimed(row["claimed_at"], now)
            )

    def _put(self, conversation_id: str, record: str, expires_at: datetime) -> None:
        with self._lock:
            previous = self._rows.pop(conversation_id, None)
            self._rows[conversation_id] = {
                "record": record,
                "version": (previous["version"] + 1) if previous else 1,
                "expires_at": expires_at,
                "claimed_at": None,
            }
            while len(self._rows) > self._limit:
                self._rows.popitem(last=False)

    def _claim(self, conversation_id: str, version: int) -> bool:
        now = _utcnow()
        with self._lock:
            row = self._rows.get(conversation_id)
            if (
                row is None
                or row["version"] != version
                or _claimed(row["claimed_at"], now)
            ):
                return False
            row["version"] += 1
            row["claimed_at"] = now
            return True

    def _release(self, conversation_id: str) -> None:
        with self._lock:
            row = self._rows.get(conversation_id)
            if row is not None:
                row["claimed_at"] = None

    def _clear(self, conversation_id: str) -> None:
        with self._lock:
            self._rows.pop(conversation_id, None)


class _ManagedPendingBase(_PendingQuestions):
    """A SQL table of paused conversations, every statement scoped to one deployment by ``mine``."""

    def __init__(
        self,
        engine: Any,
        table: Any,
        mine: Callable[[Any, Any], Any],
        deployment: str,
    ):
        self._engine = engine
        self._table = table
        self._mine = mine
        # for the one statement mine cannot scope: the INSERT
        self._deployment = deployment

    def _row(self, conversation_id: str):
        return self._mine(self._table.select(), self._table).where(
            self._table.c.conversation_id == conversation_id
        )

    def _update(self, conversation_id: str):
        return self._mine(self._table.update(), self._table).where(
            self._table.c.conversation_id == conversation_id
        )

    def _get(self, conversation_id: str) -> _Pending | None:
        now = _utcnow()
        with self._engine.connect() as conn:
            row = conn.execute(self._row(conversation_id)).fetchone()
        if row is None or row.expires_at < now:
            return None
        return _Pending(row.request, row.version, _claimed(row.claimed_at, now))

    def _put(self, conversation_id: str, record: str, expires_at: datetime) -> None:
        now = _utcnow()
        t = self._table
        values = {
            "request": record,
            "version": t.c.version + 1,
            "claimed_at": None,
            "expires_at": expires_at,
            "updated_at": now,
        }
        with self._engine.begin() as conn:
            if conn.execute(self._update(conversation_id).values(**values)).rowcount:
                return
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    t.insert().values(
                        deployment_id=self._deployment,
                        conversation_id=conversation_id,
                        request=record,
                        version=1,
                        claimed_at=None,
                        expires_at=expires_at,
                        created_at=now,
                        updated_at=now,
                    )
                )
        except Exception:  # noqa: BLE001 -- another replica inserted first: update theirs
            with self._engine.begin() as conn:
                updated = conn.execute(
                    self._update(conversation_id).values(**values)
                ).rowcount
            if not updated:
                # The insert failed for its own reason (the table is gone, the
                # store is down). Swallowed, the question would be asked and
                # never recorded, and the answer refused with nothing_to_resume.
                raise

    def _claim(self, conversation_id: str, version: int) -> bool:
        from sqlalchemy import or_  # noqa: PLC0415

        now = _utcnow()
        t = self._table
        with self._engine.begin() as conn:
            taken = conn.execute(
                self._update(conversation_id)
                .where(t.c.version == version)
                .where(
                    or_(t.c.claimed_at.is_(None), t.c.claimed_at <= now - CLAIM_LEASE)
                )
                .values(version=t.c.version + 1, claimed_at=now, updated_at=now)
            ).rowcount
        return taken == 1

    def _release(self, conversation_id: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                self._update(conversation_id).values(
                    claimed_at=None, updated_at=_utcnow()
                )
            )

    def _clear(self, conversation_id: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                self._mine(self._table.delete(), self._table).where(
                    self._table.c.conversation_id == conversation_id
                )
            )
