"""Model-callable memory tools.

``remember`` / ``recall`` / ``forget`` are plain functions the agent's own LLM
decides to call — the MemGPT shape, and the reason there is no extraction model
in the SDK: the agent already knows what is worth keeping, so nothing has to
guess on its behalf.

They take no store, subject, or conversation argument. Those come from the
per-request context, so the signature the model sees carries no plumbing it
would have to invent values for. Register them with
``agent_app.memory_tools(framework)``.
"""

from __future__ import annotations

import inspect
import json
import logging
import re
from typing import get_type_hints

from .memory import WRITABLE_SCOPES, WRITTEN_BY_AGENT


log = logging.getLogger(__name__)

_NO_CONTEXT = (
    "Memory is unavailable in this call, so nothing was stored. Continue "
    "without it and do not retry."
)
_NO_IDENTITY_CONTEXT = (
    "This conversation cannot be labelled right now, so nothing was recorded. "
    "Continue without it and do not retry."
)


def _context():
    """The active turn's context, or None.

    Separate from :func:`_resolve` because not every tool needs memory:
    ``identify`` labels a conversation and an agent may do that with no memory
    store configured at all.
    """
    from .autoevents import current_context

    ctx = current_context.get(None)
    if ctx is None:
        log.warning("Tool called with no active request context")
    return ctx


def _resolve():
    """The active turn's (memory, ctx), or (None, None).

    The context is a contextvar set on the request's event loop. A tool body
    running in a thread the loop did not hand off to — ``run_in_executor``
    rather than ``to_thread`` — will not see it. That degrades to a logged
    no-op returning a sentence the model can act on, rather than raising inside
    a tool call, which the model would read as a tool failure and retry.
    """
    from .autoevents import current_context

    ctx = current_context.get(None)
    if ctx is None or ctx.memory is None:
        log.warning("Memory tool called with no active request context")
        return None, None
    return ctx.memory, ctx


def _owner(ctx, scope: str) -> str:
    return ctx.conversation_id if scope == "session" else ctx.subject


#: Words too common to make two keys related. Without this, every key sharing
#: "user" or "customer" would be reported against every other one.
_STOPWORDS = frozenset(
    {"the", "a", "of", "for", "to", "is", "user", "users", "my", "their"}
)
#: How many existing keys to name back. Enough to spot a duplicate, not enough
#: to turn a tool result into a memory dump.
_MAX_RELATED = 5


def _normalize_key(key: str) -> str:
    """One spelling per key, so case and spacing cannot fork a memory.

    ``Preferred Language`` and ``preferred_language`` are the same fact, and
    storing both is how a store ends up disagreeing with itself. Applied to the
    model-facing tools only; programmatic callers of ``set_state`` keep whatever
    key they chose, because they are not guessing.
    """
    return "_".join(str(key).strip().lower().split())


def _tokens(key: str) -> set[str]:
    return {
        w for w in re.split(r"[^a-z0-9]+", key.lower()) if w and w not in _STOPWORDS
    }


def _related_keys(memory, scope: str, owner: str, key: str) -> list[str]:
    """Existing keys that look like they might mean the same thing.

    Token overlap, not embeddings: it is free, it runs on the write path, and it
    catches the case that actually occurs — the model coining a synonym of a key
    it already wrote. Best-effort; a failure here must not block the write.
    """
    try:
        existing = memory.list_state(scope, owner)
    except Exception:  # noqa: BLE001
        log.debug("Could not list state for duplicate check", exc_info=True)
        return []
    wanted = _tokens(key)
    if not wanted:
        return []
    related = [
        row["key"]
        for row in existing
        if row["key"] != key and _tokens(row["key"]) & wanted
    ]
    return related[:_MAX_RELATED]


def _check_scope(scope: str) -> str | None:
    if scope not in WRITABLE_SCOPES:
        return (
            f"Cannot write scope {scope!r}. Use 'user' for things true about "
            "this person across conversations, or 'session' for this "
            "conversation only."
        )
    return None


def remember(key: str, value: str, scope: str = "user") -> str:
    """Store a durable fact so it is available in later conversations.

    Use this for things that stay true — preferences, identifiers, goals,
    corrections the user made. Use ``scope='session'`` for working notes that
    only matter in this conversation.

    Args:
        key: short stable identifier, e.g. "preferred_language".
        value: the fact, written so it is useful without the surrounding chat.
        scope: "user" (default, persists across conversations) or "session".
    """
    memory, ctx = _resolve()
    if memory is None:
        return _NO_CONTEXT
    problem = _check_scope(scope)
    if problem:
        return problem
    # provenance points at the turn, so "why does the agent believe this?"
    # resolves to a real row rather than to ids that may not exist yet
    source_ref = json.dumps(
        {
            "conversation_id": ctx.conversation_id,
            "turn_id": ctx.turn_id,
            "message_id": ctx.message_id,
        }
    )
    owner = _owner(ctx, scope)
    key = _normalize_key(key)
    # Read before write, so the model sees what it is about to sit beside.
    neighbours = _related_keys(memory, scope, owner, key)
    memory.set_state(
        scope,
        owner,
        key,
        value,
        source_ref=source_ref,
        written_by=WRITTEN_BY_AGENT,
    )
    stored = f"Stored {key!r} in {scope} memory."
    if neighbours:
        # Nothing is merged automatically — the model knows which of these means
        # the same thing and this store does not. Naming them is what stops
        # customer_name, customer_first_name and name accumulating as three
        # rows that disagree, which is the failure mode of upserting on exact
        # key match alone.
        listed = ", ".join(repr(n) for n in neighbours)
        stored += (
            f" You already store {listed} for this user — if any of those mean "
            f"the same thing as {key!r}, `forget` the ones you are replacing."
        )
    return stored


def recall(key: str, scope: str = "user") -> str:
    """Look up a fact stored earlier with ``remember``.

    Args:
        key: the identifier used when it was stored.
        scope: "user" (default) or "session".
    """
    memory, ctx = _resolve()
    if memory is None:
        return _NO_CONTEXT
    # same normalization as remember, or a key the model stored is unreachable
    # by the spelling it used to store it
    key = _normalize_key(key)
    value = memory.get_state(scope, _owner(ctx, scope), key)
    if value is None:
        return f"Nothing stored under {key!r} in {scope} memory."
    return value


def forget(key: str, scope: str = "user") -> str:
    """Delete a fact stored earlier, e.g. when the user says it is wrong.

    Args:
        key: the identifier used when it was stored.
        scope: "user" (default) or "session".
    """
    memory, ctx = _resolve()
    if memory is None:
        return _NO_CONTEXT
    problem = _check_scope(scope)
    if problem:
        return problem
    key = _normalize_key(key)
    removed = memory.delete_state(scope, _owner(ctx, scope), key)
    if not removed:
        return f"Nothing stored under {key!r} in {scope} memory."
    return f"Forgot {key!r}."


def search(query: str) -> str:
    """Search earlier conversations for anything relevant to a topic.

    Use this when the answer might depend on something discussed before that is
    no longer in the visible history. Searches only this user's own past.

    Args:
        query: what to look for, in natural language.
    """
    memory, ctx = _resolve()
    if memory is None:
        return _NO_CONTEXT
    hits = memory.search(query, subject=ctx.subject)
    if not hits:
        return f"Nothing found in earlier conversations about {query!r}."
    lines = []
    for hit in hits:
        when = (hit.get("created_at") or "")[:10]
        # timestamp and speaker are deliberately included: the model needs to
        # know how old a memory is to weigh it against the current turn
        lines.append(f"[{when} {hit.get('role', '?')}] {hit['content']}")
    return "\n".join(lines)


MEMORY_TOOLS = (remember, recall, forget, search)


def identify(subject: str) -> str:
    """Record who you are talking to, once they have identified themselves.

    Call this when the user gives you the identifier your instructions say to
    ask for — a customer key, an account number, a reference. It only labels
    the conversation so a support engineer can find it later; it does not look
    anything up and does not change what you can remember or recall.

    Args:
        subject: the identifier exactly as the user gave it.
    """
    ctx = _context()
    if ctx is None:
        return _NO_IDENTITY_CONTEXT
    subject = (subject or "").strip()
    if not subject:
        return "No identifier was given, so nothing was recorded."
    # deliberately record_subject and not rebind_subject: rebinding decides
    # whose durable memory this conversation may read, and the caller here is a
    # model acting on text a user typed. See context.record_subject.
    if not ctx.record_subject(subject, source="model"):
        return (
            "The identifier could not be recorded. Continue with the "
            "conversation and do not retry."
        )
    return f"Recorded this conversation as being with {subject}."


#: Registered separately from the memory tools. Identity is the one thing in
#: this module that a model asserting the wrong value could make an audit
#: surface lie about, so an app has to ask for it rather than receive it with
#: everything else.
IDENTITY_TOOLS = (identify,)


def _normalize_framework(framework: str | None) -> str:
    if framework is None:
        return "plain"
    value = str(framework).strip().lower()
    aliases = {
        "langchain": "langgraph",
        "openai-agents": "openai_agents",
        "openaiagents": "openai_agents",
        "claude-agents": "claude_agents",
        "claudeagents": "claude_agents",
        "claude-agent-sdk": "claude_agents",
        "claude_agent_sdk": "claude_agents",
    }
    return aliases.get(value, value)


def _select(include):
    """The named subset of MEMORY_TOOLS, in the canonical order."""
    if include is None:
        return list(MEMORY_TOOLS)
    by_name = {fn.__name__: fn for fn in MEMORY_TOOLS}
    unknown = [name for name in include if name not in by_name]
    if unknown:
        raise ValueError(
            f"Unknown memory tool(s) {unknown}. Available: {sorted(by_name)}."
        )
    return [fn for fn in MEMORY_TOOLS if fn.__name__ in set(include)]


def _description(fn) -> str:
    return inspect.getdoc(fn) or fn.__name__.replace("_", " ")


def _claude_input_schema(fn) -> dict[str, object]:
    schema: dict[str, object] = {}
    try:
        type_hints = get_type_hints(fn)
    except Exception:  # noqa: BLE001
        type_hints = {}
    for name, param in inspect.signature(fn).parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        annotation = type_hints.get(name, param.annotation)
        if annotation is inspect.Signature.empty:
            annotation = str
        schema[name] = annotation
    return schema


def _as_claude_agent_tool(fn, sdk_tool):
    params = inspect.signature(fn).parameters

    async def invoke(args):
        values = {}
        for name, param in params.items():
            if name in args:
                values[name] = args[name]
            elif param.default is inspect.Signature.empty:
                raise ValueError(f"Missing required argument {name!r}.")
        result = fn(**values)
        return {"content": [{"type": "text", "text": str(result)}]}

    invoke.__name__ = fn.__name__
    invoke.__doc__ = fn.__doc__
    return sdk_tool(fn.__name__, _description(fn), _claude_input_schema(fn))(invoke)


def memory_tools(framework: str = "plain", include=None):
    """The memory tools wrapped in a framework's tool type.

    One line to register, but still explicit — the SDK cannot reach into an
    arbitrary framework's tool list, and quietly appending tools to someone's
    agent would be worse than asking::

        agent = create_react_agent(llm, [*my_tools, *app.memory_tools("langgraph")])

    ``include`` registers a subset by name. Reach for it when the app has its
    own opinion about what may be written where, because ``remember`` defaults
    to ``user`` scope owned by ``ctx.subject`` — a model that decides a fact is
    worth keeping will write it there, across conversations, whatever the app
    does elsewhere. An agent that scopes identity to a single conversation
    should not also offer a tool that can promote that identity to the subject::

        app.memory_tools("langgraph", include=("recall", "search"))

    A system-prompt rule is not a substitute: the tool is callable whether or
    not the prompt mentions it.
    """
    return _wrap(_select(include), framework, "memory_tools")


def identity_tools(framework: str = "plain"):
    """``identify`` wrapped in a framework's tool type.

    Register it when the agent is expected to ask who it is talking to::

        agent = create_react_agent(llm, [*my_tools, *app.identity_tools("langgraph")])

    Not part of :func:`memory_tools`, and not for lack of tidiness: a model
    that calls this writes what an audit surface will show as the person on the
    other end of the conversation, so an app opts in to that rather than
    receiving it alongside four tools it asked for.
    """
    return _wrap(list(IDENTITY_TOOLS), framework, "identity_tools")


def _wrap(tools, framework, caller):
    framework = _normalize_framework(framework)
    if framework in ("plain", "custom"):
        return tools
    if framework == "langgraph":
        try:
            from langchain_core.tools import tool as lc_tool
        except ImportError as err:
            raise ImportError(
                f"{caller}('langgraph') requires langchain-core: "
                "pip install langchain-core"
            ) from err
        return [lc_tool(fn) for fn in tools]
    if framework == "llamaindex":
        try:
            from llama_index.core.tools import FunctionTool
        except ImportError as err:
            raise ImportError(
                f"{caller}('llamaindex') requires llama-index-core: "
                "pip install llama-index-core"
            ) from err
        return [FunctionTool.from_defaults(fn=fn) for fn in tools]
    if framework == "openai_agents":
        try:
            from agents import function_tool
        except ImportError as err:
            raise ImportError(
                f"{caller}('openai_agents') requires openai-agents: "
                "pip install openai-agents"
            ) from err
        return [function_tool(fn) for fn in tools]
    if framework == "claude_agents":
        try:
            from claude_agent_sdk import tool as sdk_tool
        except ImportError as err:
            raise ImportError(
                f"{caller}('claude_agents') requires claude-agent-sdk: "
                "pip install claude-agent-sdk"
            ) from err
        return [_as_claude_agent_tool(fn, sdk_tool) for fn in tools]
    raise ValueError(
        f"Unknown framework {framework!r}. Supported: 'langgraph', "
        "'llamaindex', 'openai_agents', 'claude_agents', 'plain' "
        "(bare functions to wrap yourself)."
    )
