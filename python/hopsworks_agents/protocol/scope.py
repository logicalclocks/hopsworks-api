"""Private mode: one caller's conversations and memory kept apart from another's.

An agent serves everyone who holds a credential for it.
By default (``memory_routes=True``) they share it: any of them may list and
read the deployment's conversations, as the chat panel's memory view does.
An agent built with ``memory_routes=False`` is **private**: what a caller
opened is the caller's alone, over A2A and over ``/v1/run`` and ``/v1/chat``
alike.

Who the caller is comes from the gateway, never from the request: the
authenticator in front of a deployment sets ``x-hopsworks-principal`` to a
stable id of the credential it verified and strips any copy the client sent.
A private agent refuses a request that arrives without it, rather than taking
the caller for an anonymous one, so a misconfigured gateway cannot merge every
caller into one.

The separation is made at the storage boundary, in one place: every
conversation id and subject is namespaced by the caller on its way into the
memory store and the pending-questions store, and the namespace is taken off
on the way out.
The handler and the wire keep seeing the plain ids, so no agent code changes.
"""

from __future__ import annotations

import os
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from .memory import ChatMemory
from .pending import TASK_KEY_SEPARATOR, _PendingQuestions


if TYPE_CHECKING:
    from datetime import datetime

    from .pending import _Pending


#: Who the gateway verified the caller to be.
PRINCIPAL_HEADER = "x-hopsworks-principal"
#: The caller of a local run, which has no gateway to say who it is.
ANONYMOUS = "anonymous"
#: What separates the caller from the id it namespaces.
#: A conversation id may carry it; the namespace is recognised as a prefix.
SCOPE_SEPARATOR = "/"

#: The caller of the request being served: set where a request enters (the
#: HTTP gate, the A2A executor) and read by the scoped stores.
_principal: ContextVar[str | None] = ContextVar(
    "hopsworks_agent_principal", default=None
)


class PrincipalRequired(Exception):  # noqa: N818 -- a refusal, named for what is missing
    """A private-mode request reached the agent without the gateway's principal."""


def _on_platform() -> bool:
    """Whether this is a deployment rather than a local run: the platform sets ``DEPLOYMENT_ID``."""
    return bool(os.environ.get("DEPLOYMENT_ID"))


def _header_principal(headers: Any) -> str | None:
    """The verified caller a request names, or None when the gateway said nothing."""
    if not headers:
        return None
    try:
        value = headers.get(PRINCIPAL_HEADER)
    except AttributeError:
        return None
    value = str(value).strip() if value else ""
    return value or None


def _principal_of(headers: Any) -> str:
    """Who a private-mode request is from.

    Raises:
        PrincipalRequired: On a deployment, when the gateway's header is missing.
    """
    principal = _header_principal(headers)
    if principal is not None:
        return principal
    if _on_platform():
        raise PrincipalRequired
    return ANONYMOUS


def _current() -> str | None:
    """The caller the current request is scoped to, or None outside private mode."""
    return _principal.get()


def _scoped(value: str) -> str:
    """An id as the store keeps it: under the current caller, when there is one."""
    principal = _principal.get()
    if principal is None or value is None:
        return value
    return f"{principal}{SCOPE_SEPARATOR}{value}"


def _plain(value: Any) -> Any:
    """An id as the handler and the wire see it: without the current caller's namespace."""
    principal = _principal.get()
    if principal is None or not isinstance(value, str):
        return value
    prefix = principal + SCOPE_SEPARATOR
    return value[len(prefix) :] if value.startswith(prefix) else value


def _mine(value: Any) -> bool:
    """Whether a stored id is in the current caller's namespace."""
    principal = _principal.get()
    if principal is None:
        return True
    return isinstance(value, str) and value.startswith(principal + SCOPE_SEPARATOR)


#: The keys of a row that hold an id the scope namespaced.
_ID_KEYS = ("conversation_id", "subject", "owner")


def _plain_row(row: Any) -> Any:
    """A row a store returned, with the namespace taken off its ids."""
    if not isinstance(row, dict) or _principal.get() is None:
        return row
    cleaned = dict(row)
    for key in _ID_KEYS:
        if key in cleaned:
            cleaned[key] = _plain(cleaned[key])
    return cleaned


def _state_owner(scope: str, owner: str) -> str:
    """The owner of a state row: a conversation's and a person's are the caller's own; the agent's is shared."""
    return _scoped(owner) if scope in ("session", "user") else owner


class _ScopedMemory(ChatMemory):
    """A memory store seen through the current caller: every id namespaced in, plain out.

    Wraps the app's store when the agent is private.
    With no caller set (a background sweep, the readiness probe) it is the
    store itself, unchanged.
    """

    def __init__(self, inner: ChatMemory):
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        # what the app probes on a store by name (_summarize, _long_term, ...)
        return getattr(self._inner, name)

    # region the turn lifecycle

    def get(self, conversation_id: str) -> list:
        return self._inner.get(_scoped(conversation_id))

    def begin_turn(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        self._inner.begin_turn(
            _scoped(conversation_id),
            turn_id,
            role,
            content,
            message_id=message_id,
            subject=_scoped(subject) if subject else subject,
        )

    def record_item(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        **kwargs: Any,
    ) -> None:
        if kwargs.get("subject"):
            kwargs["subject"] = _scoped(kwargs["subject"])
        self._inner.record_item(
            _scoped(conversation_id), turn_id, role, content, **kwargs
        )

    def end_turn(self, conversation_id: str, turn_id: str, **kwargs: Any) -> None:
        self._inner.end_turn(_scoped(conversation_id), turn_id, **kwargs)

    def clear(self, conversation_id: str) -> None:
        self._inner.clear(_scoped(conversation_id))

    def append(self, conversation_id: str, role: str, content: str) -> None:
        self._inner.append(_scoped(conversation_id), role, content)

    # endregion

    # region reading a conversation

    def list_conversations(
        self, *, subject: str | None = None, limit: int = 50
    ) -> list[dict]:
        if _principal.get() is None:
            return self._inner.list_conversations(subject=subject, limit=limit)
        # The store lists by recency across callers; the caller's own are
        # picked out of a wider page. A caller with more recent conversations
        # than the page holds sees the most recent of them.
        rows = self._inner.list_conversations(
            subject=_scoped(subject) if subject else None, limit=max(limit * 20, 1000)
        )
        mine = [_plain_row(row) for row in rows if _mine(row.get("conversation_id"))]
        return mine[:limit]

    def conversation_subject(self, conversation_id: str) -> str | None:
        return _plain(self._inner.conversation_subject(_scoped(conversation_id)))

    def rebind_turn_subject(
        self, conversation_id: str, turn_id: str, subject: str
    ) -> None:
        self._inner.rebind_turn_subject(
            _scoped(conversation_id), turn_id, _scoped(subject)
        )

    def transcript(
        self, conversation_id: str, *, include_events: bool = False
    ) -> list[dict]:
        return [
            _plain_row(row)
            for row in self._inner.transcript(
                _scoped(conversation_id), include_events=include_events
            )
        ]

    def summarized_through(self, conversation_id: str) -> int:
        return self._inner.summarized_through(_scoped(conversation_id))

    def get_summary(self, conversation_id: str) -> str | None:
        return self._inner.get_summary(_scoped(conversation_id))

    def clear_summary(self, conversation_id: str) -> bool:
        return self._inner.clear_summary(_scoped(conversation_id))

    async def maybe_summarize(self, conversation_id: str) -> bool:
        return await self._inner.maybe_summarize(_scoped(conversation_id))

    # endregion

    # region semantic memory

    def set_state(
        self, scope: str, owner: str, key: str, value: str, **kwargs: Any
    ) -> None:
        self._inner.set_state(scope, _state_owner(scope, owner), key, value, **kwargs)

    def get_state(self, scope: str, owner: str, key: str) -> str | None:
        return self._inner.get_state(scope, _state_owner(scope, owner), key)

    def list_state(self, scope: str, owner: str, **kwargs: Any) -> list[dict]:
        return [
            _plain_row(row)
            for row in self._inner.list_state(
                scope, _state_owner(scope, owner), **kwargs
            )
        ]

    def delete_state(self, scope: str, owner: str, key: str | None = None) -> int:
        return self._inner.delete_state(scope, _state_owner(scope, owner), key)

    def state_block(self, subject: str, conversation_id: str) -> str:
        return self._inner.state_block(_scoped(subject), _scoped(conversation_id))

    # endregion

    # region episodic memory

    def search(
        self,
        query: str,
        *,
        subject: str | None = None,
        conversation_id: str | None = None,
        k: int = 5,
    ) -> list[dict]:
        if _principal.get() is not None and not subject and not conversation_id:
            # a search across everything would cross callers: nothing to
            # search within means nothing found
            return []
        return [
            _plain_row(row)
            for row in self._inner.search(
                query,
                subject=_scoped(subject) if subject else None,
                conversation_id=_scoped(conversation_id) if conversation_id else None,
                k=k,
            )
        ]

    async def ingest_turn(self, conversation_id: str, turn_id: str) -> int:
        return await self._inner.ingest_turn(_scoped(conversation_id), turn_id)

    def purge_vectors(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        return self._inner.purge_vectors(
            conversation_id=_scoped(conversation_id) if conversation_id else None,
            subject=_scoped(subject) if subject else None,
        )

    # endregion

    # region the store itself

    def _pending_questions(self) -> _PendingQuestions | None:
        inner = self._inner._pending_questions()
        return _ScopedPending(inner) if inner is not None else None

    def healthcheck(self) -> bool:
        return self._inner.healthcheck()

    def maybe_reap(self) -> None:
        self._inner.maybe_reap()

    # endregion


class _ScopedPending(_PendingQuestions):
    """The pending-questions store seen through the current caller.

    A record's key is a conversation id, or a context id and a task id; the
    namespace goes in front of either, so one caller's answer can never claim
    another's question.
    """

    def __init__(self, inner: _PendingQuestions):
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _get(self, conversation_id: str) -> _Pending | None:
        return self._inner._get(_scoped(conversation_id))

    def _put(self, conversation_id: str, record: str, expires_at: datetime) -> None:
        self._inner._put(_scoped(conversation_id), record, expires_at)

    def _claim(self, conversation_id: str, version: int) -> bool:
        return self._inner._claim(_scoped(conversation_id), version)

    def _renew(self, conversation_id: str) -> bool:
        return self._inner._renew(_scoped(conversation_id))

    def _release(self, conversation_id: str) -> None:
        self._inner._release(_scoped(conversation_id))

    def _clear(self, conversation_id: str) -> None:
        self._inner._clear(_scoped(conversation_id))

    def _clear_context(self, conversation_id: str) -> None:
        self._inner._clear_context(_scoped(conversation_id))


def _key_context(key: str) -> str:
    """The conversation a pending key belongs to, whether it is the conversation's own or a task's."""
    return key.split(TASK_KEY_SEPARATOR, 1)[0]


class _PrincipalGate:
    """ASGI middleware of a private agent: every turn and task request names its caller, or is refused.

    Sets the caller for the stores below it.
    The routes anyone may read without being someone, the card, the manifest
    and the probes, pass through unscoped.
    The Agent Card in particular is served to a request that names no caller:
    when the deployment opted into a public card
    (``HOPSWORKS_AGENT_PUBLIC_CARD=true``) the gateway sends anonymous requests
    for it, and the platform's agent directory reads it in-cluster. It is the
    public card; the extended one is asked for under ``/a2a`` and stays gated.
    A principal header that is present but empty (how the gateway blanks a
    client's copy on an unauthenticated route) names no caller either.
    gRPC calls are gated the same way, from their metadata (``a2a_grpc``).
    """

    #: Path prefixes that open or read a caller's conversations and tasks.
    GATED = ("/v1/", "/a2a")

    def __init__(self, app: Any):
        self._app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or not self._gated(scope):
            await self._app(scope, receive, send)
            return
        headers = {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in scope.get("headers") or []
        }
        try:
            principal = _principal_of(headers)
        except PrincipalRequired:
            await self._refuse(send)
            return
        token = _principal.set(principal)
        try:
            await self._app(scope, receive, send)
        finally:
            _principal.reset(token)

    def _gated(self, scope: dict) -> bool:
        if scope.get("method") == "OPTIONS":
            # a CORS preflight carries no credential and reads nothing
            return False
        path = scope.get("path") or ""
        return any(path.startswith(prefix) for prefix in self.GATED)

    @staticmethod
    async def _refuse(send: Any) -> None:
        import json  # noqa: PLC0415

        body = json.dumps(
            {
                "detail": {
                    "code": "principal_required",
                    "message": "This agent keeps each caller's conversations "
                    "apart and the request does not say who the caller is. "
                    "Reach the agent through the Hopsworks gateway.",
                    "retryable": False,
                }
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
