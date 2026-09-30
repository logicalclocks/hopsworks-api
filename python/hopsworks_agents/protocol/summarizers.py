"""Ready-made summarizers for :class:`ManagedMemoryService`.

The SDK owns *when* to summarize (the trigger, the fold cutoff, the
transaction); the model call itself is yours, so the SDK stays LLM-agnostic.
A summarizer is any callable::

    (previous_summary: str | None, turns: list[Turn]) -> str

sync or async. This module ships two so the common cases are a single line:
:func:`anthropic_summarizer` for Claude and :func:`openai_summarizer` for
OpenAI and for anything that speaks its chat-completions API (vLLM, Ollama,
a LiteLLM proxy, Azure, most hosted models) through ``base_url``. Anything
else — a rules-based compactor, a provider with its own SDK — is just a
function with that shape.
"""

from __future__ import annotations

import logging
import os


log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-haiku-4-5"

SYSTEM_PROMPT = """\
You maintain a running summary of a conversation between a user and an AI \
agent. You will be given the summary so far (which may be empty) and the turns \
that have happened since. Return an updated summary that folds the new turns \
into the old one.

Write for the agent that will read this as its only memory of the earlier \
conversation:

- Keep facts the user stated about themselves, their data, their goals, and \
their preferences. These are the whole point — losing one means the agent \
asks again.
- Keep decisions, conclusions, and anything the user corrected you on.
- Keep unresolved threads: open questions, things the user said they would \
come back to.
- Drop pleasantries, restatements, and detail that no longer changes what the \
agent would do next.
- Write plain prose or short bullets. No preamble, no "here is the summary", \
no meta-commentary about summarizing.

Stay under roughly 400 words. When the summary is at risk of growing past \
that, compress the oldest material further rather than dropping recent \
material.\
"""


def _format_turns(turns) -> str:
    return "\n\n".join(f"{t['role']}: {t['content']}" for t in turns)


def _user_message(previous: str | None, turns) -> str:
    prior = previous or "(none — this is the first summary)"
    return (
        f"<summary_so_far>\n{prior}\n</summary_so_far>\n\n"
        f"<new_turns>\n{_format_turns(turns)}\n</new_turns>\n\n"
        "Return the updated summary."
    )


def _key_resolver(api_key: str | None, env_var: str, api_key_secret: str | None):
    """Explicit key → the environment → the Hopsworks secret.

    Resolved on first use rather than at construction so importing never
    reaches for a credential. The secret hop is how a deployment reuses the key
    its agent already has for its own model instead of provisioning a second
    one for summarization.
    """

    def resolve() -> str | None:
        if api_key:
            return api_key
        env = os.environ.get(env_var)
        if env:
            return env
        secret = api_key_secret or os.environ.get(f"{env_var}_SECRET_NAME")
        if not secret:
            return None
        try:
            import hopsworks

            return hopsworks.get_secrets_api().get(secret)
        except Exception:  # noqa: BLE001 — fall through to the SDK's own resolution
            log.exception("Could not read the key from Hopsworks secret %s", secret)
            return None

    return resolve


def anthropic_summarizer(
    model: str = DEFAULT_MODEL,
    *,
    api_key: str | None = None,
    api_key_secret: str | None = None,
    max_tokens: int = 1024,
    system_prompt: str = SYSTEM_PROMPT,
):
    """An async summarizer backed by the Claude API.

        memory = ManagedMemoryService(summarize=anthropic_summarizer())

    Requires ``pip install anthropic``.

    The key is resolved once, on first use rather than at construction, so
    importing this never reaches for a credential: explicit ``api_key`` →
    ``ANTHROPIC_API_KEY`` → the Hopsworks secret named by ``api_key_secret`` (or
    ``ANTHROPIC_API_KEY_SECRET_NAME``). That last hop is how a deployment reuses
    the same secret the agent already uses for its own model, instead of
    provisioning a second one for summarization.

    Defaults to Haiku 4.5: this is a bounded rewrite of text the agent already
    produced, and the cost lands on every Nth turn of every conversation. Pass
    ``model=`` for anything else. Not on Claude? See :func:`openai_summarizer`.
    """
    try:
        import anthropic  # noqa: F401
    except ImportError as err:
        raise ImportError(
            "anthropic_summarizer requires the Anthropic SDK: pip install anthropic"
        ) from err

    client = None
    _resolve_key = _key_resolver(api_key, "ANTHROPIC_API_KEY", api_key_secret)

    async def summarize(previous: str | None, turns) -> str:
        nonlocal client
        if client is None:
            import anthropic

            key = _resolve_key()
            client = (
                anthropic.AsyncAnthropic(api_key=key)
                if key
                else anthropic.AsyncAnthropic()
            )

        response = await client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": _user_message(previous, turns)}],
        )
        return "".join(
            block.text for block in response.content if block.type == "text"
        ).strip()

    return summarize


OPENAI_DEFAULT_MODEL = "gpt-4o-mini"


def openai_summarizer(
    model: str = OPENAI_DEFAULT_MODEL,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    api_key_secret: str | None = None,
    max_tokens: int = 1024,
    system_prompt: str = SYSTEM_PROMPT,
    **client_kwargs,
):
    """An async summarizer over the OpenAI chat-completions API.

        memory = ManagedMemoryService(summarize=openai_summarizer())

    Requires ``pip install openai``.

    ``base_url`` points it at any server that speaks the same API — a vLLM or
    Ollama instance in the cluster, a LiteLLM proxy, Azure, a hosted model —
    with ``model`` naming what that server serves::

        openai_summarizer("llama-3.1-8b", base_url="http://vllm.default:8000/v1", api_key="none")

    Extra keyword arguments (``default_headers``, ``timeout``, ...) go to the
    ``AsyncOpenAI`` client. The key resolves once, on first use: explicit
    ``api_key`` → ``OPENAI_API_KEY`` → the Hopsworks secret named by
    ``api_key_secret`` (or ``OPENAI_API_KEY_SECRET_NAME``), so a deployment can
    reuse the secret its agent already holds. ``OPENAI_BASE_URL`` is honoured
    by the SDK itself when ``base_url`` is not given.

    Defaults to gpt-4o-mini for the same reason the Claude one defaults to
    Haiku: a bounded rewrite that lands on every Nth turn of every conversation.
    """
    try:
        import openai  # noqa: F401
    except ImportError as err:
        raise ImportError(
            "openai_summarizer requires the OpenAI SDK: pip install openai"
        ) from err

    client = None
    _resolve_key = _key_resolver(api_key, "OPENAI_API_KEY", api_key_secret)

    async def summarize(previous: str | None, turns) -> str:
        nonlocal client
        if client is None:
            import openai

            kwargs = dict(client_kwargs)
            if base_url:
                kwargs["base_url"] = base_url
            key = _resolve_key()
            if key:
                kwargs["api_key"] = key
            client = openai.AsyncOpenAI(**kwargs)

        response = await client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": _user_message(previous, turns)},
            ],
        )
        return (response.choices[0].message.content or "").strip()

    return summarize


def sentence_transformer_embedder(
    model_name: str = "all-MiniLM-L6-v2", *, normalize: bool = True
):
    """The shipped default embedder: a local sentence-transformers model.

        memory = ManagedMemoryService(embedder=sentence_transformer_embedder(), ...)

    Requires ``pip install sentence-transformers``.

    Local rather than hosted on purpose. Anthropic has no embeddings endpoint,
    so unlike ``summarize`` there is no API counterpart to reach for — and
    embedding every ingested message against a remote service would put a
    network call on the write path of every turn.

    The returned callable carries ``dimension`` and ``model_id``, which
    :func:`hopsworks_agents.protocol.vectorstore.vector_store_for` uses to size
    the feature group and to detect a swapped model later.
    """
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as err:
        raise ImportError(
            "sentence_transformer_embedder requires sentence-transformers: "
            "pip install sentence-transformers"
        ) from err

    model = SentenceTransformer(model_name)

    def embed(text: str) -> list[float]:
        return model.encode(text, normalize_embeddings=normalize).tolist()

    embed.dimension = model.get_sentence_embedding_dimension()
    embed.model_id = model_name
    return embed
