"""One model call through Claude Code, for a pod that holds a Claude Code login and no API key.

An agent deployment, an evaluation job or a feedback review may run on the Claude Code
login the user connected (``CLAUDE_CODE_OAUTH_TOKEN``, a token from ``claude setup-token``)
rather than on an Anthropic API key.
That token is honoured by Claude Code alone, so the summariser and the judges cannot put
it on the Messages API; they run the one call they need as a Claude Code session instead,
through the Claude Agent SDK, with every built-in tool off.
This module is that call.
"""

from __future__ import annotations

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any


_logger = logging.getLogger(__name__)

#: The variable Claude Code reads its login from, set by the platform from the user's
#: ``claude_code_oauth_token`` secret.
LOGIN_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
#: The levels Claude Code accepts for ``effort``.
EFFORTS = ("low", "medium", "high", "xhigh", "max")
#: How long one call may take, start to finish, in seconds: the Anthropic client's own
#: default. A Claude Code session that stalls would otherwise hold the judge's worker,
#: or the turn waiting on its summary, for ever: ``max_turns`` bounds turns, not time.
TIMEOUT_S = 600.0


def _login() -> str:
    """The login token in the environment, or "" when the pod holds none."""
    return os.environ.get(LOGIN_ENV) or ""


def _is_login(credential: str | None) -> bool:
    """Whether a credential resolved for Anthropic is the Claude Code login, not an API key.

    Compared with the variable rather than by shape, so a key that happens to look
    like a token is never sent down the wrong path.
    """
    return bool(credential) and credential == _login()


def _options(
    *, system_prompt: str | None, model: str, effort: str | None = None
) -> Any:
    """Options for one call: the prompt, the model, no tools, nothing read from the pod."""
    from claude_agent_sdk import ClaudeAgentOptions

    extra: dict[str, Any] = {}
    if system_prompt:
        extra["system_prompt"] = system_prompt
    if effort in EFFORTS:
        extra["effort"] = effort
    return ClaudeAgentOptions(
        model=model, tools=[], setting_sources=[], max_turns=1, **extra
    )


async def _reply(
    prompt: str,
    *,
    model: str,
    system_prompt: str | None = None,
    effort: str | None = None,
    timeout_s: float = TIMEOUT_S,
) -> str:
    """The text Claude Code answers ``prompt`` with, within ``timeout_s`` seconds.

    A call that cannot reach the model ends with the SDK raising "Claude Code returned an
    error result: success", the result's subtype rather than the cause.
    The cause arrives one message earlier as a synthetic assistant message carrying
    ``error`` ("Failed to authenticate. API Error: 401 ..."), so it is kept and raised in
    the SDK line's place.

    The deadline is an anyio cancel scope rather than ``asyncio.wait_for``: the SDK
    shields the transport's close from anyio cancellation, so the Claude Code process
    is terminated on the way out, where a raw asyncio cancellation would skip that and
    leave it running until the pod exits.
    The stream is closed on every exit so the same holds for an error raised mid-run.
    """
    import anyio
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, query

    options = _options(system_prompt=system_prompt, model=model, effort=effort)
    final = ""
    last = ""
    failure = ""
    stream = query(prompt=prompt, options=options)
    try:
        with anyio.fail_after(timeout_s):
            async for message in stream:
                if isinstance(message, AssistantMessage):
                    text = "".join(
                        block.text
                        for block in message.content
                        if isinstance(block, TextBlock)
                    ).strip()
                    if getattr(message, "error", None):
                        failure = text or str(message.error)
                    elif text:
                        last = text
                elif isinstance(message, ResultMessage) and not message.is_error:
                    final = str(message.result or "")
    except TimeoutError as err:
        raise RuntimeError(
            f"Claude Code did not answer within {timeout_s:g}s; the call was stopped"
        ) from err
    except Exception as err:
        raise RuntimeError(failure or str(err)) from err
    finally:
        await stream.aclose()
    return (final or last).strip()


def _reply_sync(prompt: str, **call: Any) -> str:
    """:func:`_reply` from synchronous code, such as a judge grading a trial.

    Run on the current thread when no event loop is running there, and on a worker
    thread with a loop of its own otherwise: ``asyncio.run`` refuses to nest.
    The deadline is :func:`_reply`'s, inside the loop, so the worker ends with it and
    the wait here ends with the worker; a deadline on the wait alone would leave the
    worker, and the Claude Code process under it, running.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_reply(prompt, **call))
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, _reply(prompt, **call)).result()
