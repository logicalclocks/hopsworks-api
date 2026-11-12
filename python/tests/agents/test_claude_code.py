"""The one Claude Code call the summariser and the judges share."""

from __future__ import annotations

import asyncio
import sys
import types

import pytest
from hopsworks_agents import _claude_code


def _fake_sdk(monkeypatch, query):
    """A stand-in ``claude_agent_sdk`` whose ``query`` is the given generator."""

    class ClaudeAgentOptions:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class TextBlock:
        def __init__(self, text):
            self.text = text

    class AssistantMessage:
        def __init__(self, content, error=None):
            self.content = content
            self.error = error

    class ResultMessage:
        def __init__(self, result, is_error=False):
            self.result = result
            self.is_error = is_error

    module = types.ModuleType("claude_agent_sdk")
    module.ClaudeAgentOptions = ClaudeAgentOptions
    module.TextBlock = TextBlock
    module.AssistantMessage = AssistantMessage
    module.ResultMessage = ResultMessage
    module.query = query
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", module)
    return module


class TestTheCallHasADeadline:
    """A Claude Code session that stalls must not hold a judge or a turn for ever."""

    def test_a_stream_that_never_answers_fails_at_the_deadline_and_is_closed(
        self, monkeypatch
    ):
        state = {"closed": False}

        async def query(prompt, options):
            try:
                await asyncio.sleep(3600)
                yield None
            finally:
                state["closed"] = True

        _fake_sdk(monkeypatch, query)
        loop = asyncio.new_event_loop()
        try:
            started = loop.time()
            with pytest.raises(RuntimeError, match="did not answer within 0.05s"):
                loop.run_until_complete(
                    _claude_code._reply("fold", model="haiku", timeout_s=0.05)
                )
            assert loop.time() - started < 2
        finally:
            loop.close()
        assert state["closed"], "the Claude Code stream was left open"

    def test_the_deadline_is_the_clients_ten_minutes_by_default(self, monkeypatch):
        seen = {}

        async def query(prompt, options):
            seen["prompt"] = prompt
            yield sys.modules["claude_agent_sdk"].ResultMessage("done")

        _fake_sdk(monkeypatch, query)
        assert _claude_code.TIMEOUT_S == 600.0
        assert asyncio.run(_claude_code._reply("fold", model="haiku")) == "done"
        assert seen["prompt"] == "fold"

    def test_a_stream_that_fails_mid_run_is_closed_too(self, monkeypatch):
        state = {"closed": False}

        async def query(prompt, options):
            sdk = sys.modules["claude_agent_sdk"]
            try:
                yield sdk.AssistantMessage([sdk.TextBlock("401 invalid")], error="auth")
                raise Exception("Claude Code returned an error result: success")
            finally:
                state["closed"] = True

        _fake_sdk(monkeypatch, query)
        with pytest.raises(RuntimeError, match="401 invalid"):
            asyncio.run(_claude_code._reply("fold", model="haiku"))
        assert state["closed"]

    def test_the_sync_wrapper_ends_with_the_deadline_under_a_running_loop(
        self, monkeypatch
    ):
        async def query(prompt, options):
            await asyncio.sleep(3600)
            yield None

        _fake_sdk(monkeypatch, query)

        async def grade():
            # a judge grading from inside a loop: the call runs on a worker thread
            return _claude_code._reply_sync("grade", model="sonnet", timeout_s=0.05)

        with pytest.raises(RuntimeError, match="did not answer within"):
            asyncio.run(asyncio.wait_for(grade(), timeout=5))
