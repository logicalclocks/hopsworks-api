"""Memory configured by the deployment rather than by the agent's code.

A Hopsworks agent deployment can carry a memory policy, set in the deployment
form and injected into the pod as ``AGENT_MEMORY_CONFIG``. :class:`AgentApp`
reads it only when the agent passed no ``memory=`` of its own, so code always
wins: an explicit store is used as given, and an explicit ``memory=None`` stays
off even when the deployment configures one.

Only the *policy* comes from the deployment -- whether memory is on, which
tiers, which summarizer. Storage was already the platform's to provide
(:class:`ManagedMemoryService` resolves the project database from the injected
``MYSQL_*`` variables), and *reading* memory stays the handler's: ``ctx.history``
and ``ctx.system_context()`` have to be placed in a prompt, and where context
belongs is a property of that prompt. A deployment that switches memory on
records turns; it cannot make an agent use them.

Recording is safe beside a framework's own store. A LangGraph checkpointer or a
LlamaIndex chat store, in the form most agents use, keeps history in the
process, which a deployment that scales to zero loses on every wake. What has
to stay deliberate is reading both at once: a handler that passes ``ctx.history``
*and* a thread id the framework already has history for sends the conversation
twice.

The JSON, every key optional::

    {
      "enabled": true,
      "summarize": {"provider": "hopsworks", "deployment": "my-llm"},
      "long_term": true,
      "search": {"model": "all-MiniLM-L6-v2"},
      "summarize_after_messages": 20,
      "keep_recent_messages": 10,
      "message_retention_days": 30
    }

A policy switches on every tier it does not mention, so ``{"enabled": true}``
gives semantic and episodic memory as well as the conversation itself. Set
``"long_term": false`` or ``"search": false`` to leave one out. The summarizer
is the exception: there is nothing to fold with until a provider is named.

Nothing here is fatal. A policy that cannot be honoured in full degrades to the
part that can be -- memory without a summarizer, say -- and says so in the log,
because an agent that refuses to start is worse than one that remembers less
than it was asked to.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any


log = logging.getLogger(__name__)

#: Where the platform puts the deployment's memory policy. ``AGENT_`` is the
#: agent-entrypoint contract the backend already injects `AGENT_FRAMEWORK` under.
ENV_VAR = "AGENT_MEMORY_CONFIG"

# Keys this version understands. A newer backend may send more; unknown keys are
# logged and ignored rather than refused, so an older SDK keeps working.
_KNOWN = frozenset(
    {
        "enabled",
        "summarize",
        "long_term",
        "search",
        "summarize_after_messages",
        "keep_recent_messages",
        "message_retention_days",
        "tool_event_retention_days",
        "max_messages",
    }
)

_PASSTHROUGH = (
    "summarize_after_messages",
    "keep_recent_messages",
    "message_retention_days",
    "tool_event_retention_days",
    "max_messages",
)


def _summarizer(spec: Any):
    """Build the configured summarizer, or None when it cannot be built.

    Args:
        spec: The ``summarize`` object from the policy, or a bare provider name.

    Returns:
        A summarizer callable, or None when the spec names no usable provider.
    """
    if not spec:
        return None
    if isinstance(spec, str):
        spec = {"provider": spec}
    if not isinstance(spec, dict):
        log.warning("%s: 'summarize' is neither a name nor an object; ignored", ENV_VAR)
        return None

    provider = str(spec.get("provider") or "").strip().lower()
    model = spec.get("model") or None
    secret = spec.get("api_key_secret") or None

    from . import summarizers  # noqa: PLC0415 -- optional provider SDKs

    if provider == "hopsworks":
        # An LLM served in this project: no key to configure, since the pod's
        # own serving credential reaches the inference gateway.
        deployment = spec.get("deployment") or spec.get("name")
        if not deployment:
            log.warning("%s: a hopsworks summarizer needs 'deployment'", ENV_VAR)
            return None
        return summarizers.hopsworks_summarizer(deployment, model)
    if provider == "openai":
        kwargs: dict[str, Any] = {"base_url": spec.get("base_url") or None}
        if secret:
            kwargs["api_key_secret"] = secret
        return (
            summarizers.openai_summarizer(model, **kwargs)
            if model
            else summarizers.openai_summarizer(**kwargs)
        )
    if provider == "anthropic":
        kwargs = {"api_key_secret": secret} if secret else {}
        return (
            summarizers.anthropic_summarizer(model, **kwargs)
            if model
            else summarizers.anthropic_summarizer(**kwargs)
        )

    log.warning(
        "%s: unknown summarizer provider %r; memory runs without a summary",
        ENV_VAR,
        provider,
    )
    return None


def _wants_search(spec: Any) -> bool:
    """Whether the policy asks for semantic search.

    ``{}`` means on with the default model, not off: a form that sends an object
    with nothing named in it has still ticked the box, and a backend that omits
    null fields sends exactly that. ``{"enabled": false}`` is how a form says
    off, because a backend that drops nulls cannot send ``false`` for the
    object itself.
    """
    if spec is None or spec is False:
        return False
    return not (isinstance(spec, dict) and spec.get("enabled") is False)


def _search(spec: Any) -> dict[str, Any]:
    """The embedder and vector store for semantic search over past conversations.

    Args:
        spec: ``true`` or an object, empty or naming an embedding model.

    Returns:
        The keyword arguments for :class:`ManagedMemoryService`, empty when
        search is off or its model cannot be loaded.
    """
    if not _wants_search(spec):
        return {}
    model = None
    if isinstance(spec, dict):
        model = spec.get("model") or spec.get("embedder") or None
    try:
        from . import summarizers, vectorstore  # noqa: PLC0415 -- optional extra

        embedder = (
            summarizers.sentence_transformer_embedder(model)
            if model
            else summarizers.sentence_transformer_embedder()
        )
        return {
            "embedder": embedder,
            "vector_store": vectorstore.vector_store_for(embedder),
        }
    except Exception as err:  # noqa: BLE001 -- search is the tier that can be dropped
        log.warning(
            "%s: semantic search was asked for but the embedding model could not "
            "be loaded (%s); search falls back to keywords",
            ENV_VAR,
            err,
        )
        return {}


def policy(env: dict[str, str] | None = None) -> dict[str, Any] | None:
    """The deployment's memory policy as a dict, or None when there is none.

    Args:
        env: Environment to read; the process environment by default.

    Returns:
        The parsed policy, or None when the variable is absent, empty, unreadable
        or explicitly disabled.
    """
    raw = (env if env is not None else os.environ).get(ENV_VAR, "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError as err:
        log.error("%s is not valid JSON (%s); memory stays off", ENV_VAR, err)
        return None
    if isinstance(parsed, bool):  # "true" is a legitimate shorthand for on
        parsed = {"enabled": parsed}
    if not isinstance(parsed, dict):
        log.error("%s must be a JSON object; memory stays off", ENV_VAR)
        return None
    # Absent means on: a deployment that writes a policy at all has asked for
    # memory, and `enabled: false` is how the form switches it back off.
    if not parsed.get("enabled", True):
        return None
    unknown = sorted(set(parsed) - _KNOWN)
    if unknown:
        log.info("%s: ignoring unknown key(s) %s", ENV_VAR, ", ".join(unknown))
    return parsed


def memory_from_env(env: dict[str, str] | None = None):
    """The memory store this deployment configures, or None when it configures none.

    Reads :data:`ENV_VAR`, which the platform injects from the deployment's
    memory policy, and builds a :class:`ManagedMemoryService` from it. Storage
    needs no configuration: inside a deployment the service finds the project
    database itself.

    Args:
        env: Environment to read; the process environment by default.

    Returns:
        A :class:`ManagedMemoryService`, or None.

    Example: what the deployment form writes
        ```json
        {"enabled": true, "summarize": {"provider": "hopsworks",
         "deployment": "my-llm"}, "long_term": true}
        ```
    """
    config = policy(env)
    if config is None:
        return None

    # Every tier unless the policy says otherwise. A deployment that asks for
    # memory wants an agent that remembers, and the tiers it does not name are
    # the ones nobody had an opinion about; the summarizer is the exception,
    # since there is no model to fold with until one is named.
    kwargs: dict[str, Any] = {"long_term": bool(config.get("long_term", True))}
    for key in _PASSTHROUGH:
        if config.get(key) is not None:
            kwargs[key] = config[key]
    summarize = _summarizer(config.get("summarize"))
    if summarize is not None:
        kwargs["summarize"] = summarize
    # An absent search follows long-term memory rather than defaulting to on.
    # Read as "on" it undid the form: the policy for "semantic memory off" is
    # {"enabled": true, "long_term": false} with no search key, and search
    # implies long-term, so the tier the operator had just turned off came back
    # with a vector index attached. Only a search the policy actually names can
    # promote long-term.
    requested = config.get("search")
    wants_search = kwargs["long_term"] if requested is None else _wants_search(requested)
    if wants_search:
        if requested is not None and not kwargs["long_term"]:
            # search reads the long-term store; asking for one without the other
            # is a form that let an impossible combination through
            log.info("%s: search implies long-term memory; turning it on", ENV_VAR)
            kwargs["long_term"] = True
        kwargs.update(_search(requested if requested is not None else True))

    try:
        from .memory import ManagedMemoryService  # noqa: PLC0415 -- optional extra

        store = ManagedMemoryService(**kwargs)
    except Exception as err:  # noqa: BLE001 -- never fatal; see the module docstring
        log.error(
            "%s asked for memory but the store could not be built (%s); the agent "
            "runs without it",
            ENV_VAR,
            err,
        )
        return None

    log.info(
        "memory configured by the deployment: summary=%s long_term=%s search=%s",
        summarize is not None,
        kwargs["long_term"],
        "vector_store" in kwargs,
    )
    return store
