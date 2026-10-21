"""The shipped summarizers: what they send, and where the key comes from."""

import asyncio
import sys
import types
from unittest import mock

import pytest


def _fake_openai(monkeypatch, reply="folded"):
    """A stand-in ``openai`` module recording how the client was built and called."""
    calls = {}

    class AsyncOpenAI:
        def __init__(self, **kwargs):
            calls["client"] = kwargs
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(create=self._create)
            )

        async def _create(self, **kwargs):
            calls["create"] = kwargs
            message = types.SimpleNamespace(content=reply)
            return types.SimpleNamespace(
                choices=[types.SimpleNamespace(message=message)]
            )

    module = types.ModuleType("openai")
    module.AsyncOpenAI = AsyncOpenAI
    monkeypatch.setitem(sys.modules, "openai", module)
    return calls


TURNS = [
    {"role": "user", "content": "I live in Oslo"},
    {"role": "assistant", "content": "Noted."},
]


class TestOpenAISummarizer:
    def test_sends_the_prompt_and_returns_the_reply(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import (
            SYSTEM_PROMPT,
            openai_summarizer,
        )

        calls = _fake_openai(monkeypatch)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-env")
        summarize = openai_summarizer()

        assert asyncio.run(summarize("older summary", TURNS)) == "folded"
        assert calls["client"] == {"api_key": "sk-env"}
        create = calls["create"]
        assert create["model"] == "gpt-4o-mini"
        assert create["messages"][0] == {"role": "system", "content": SYSTEM_PROMPT}
        user = create["messages"][1]["content"]
        assert "older summary" in user and "user: I live in Oslo" in user

    def test_first_summary_is_marked_as_such(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        calls = _fake_openai(monkeypatch)
        asyncio.run(openai_summarizer(api_key="k")(None, TURNS))
        assert "first summary" in calls["create"]["messages"][1]["content"]

    def test_custom_endpoint_and_client_options(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        calls = _fake_openai(monkeypatch)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        summarize = openai_summarizer(
            "llama-3.1-8b",
            base_url="http://vllm:8000/v1",
            api_key="none",
            default_headers={"X-Team": "ml"},
        )
        asyncio.run(summarize(None, TURNS))
        assert calls["client"] == {
            "base_url": "http://vllm:8000/v1",
            "api_key": "none",
            "default_headers": {"X-Team": "ml"},
        }
        assert calls["create"]["model"] == "llama-3.1-8b"

    def test_without_a_key_the_sdk_resolves_it(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        calls = _fake_openai(monkeypatch)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY_SECRET_NAME", raising=False)
        asyncio.run(openai_summarizer()(None, TURNS))
        assert "api_key" not in calls["client"]

    def test_key_comes_from_a_hopsworks_secret(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        calls = _fake_openai(monkeypatch)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        secrets = mock.MagicMock()
        secrets.get.return_value = "sk-secret"
        hopsworks = types.ModuleType("hopsworks")
        hopsworks.get_secrets_api = lambda: secrets
        monkeypatch.setitem(sys.modules, "hopsworks", hopsworks)

        asyncio.run(openai_summarizer(api_key_secret="openai-key")(None, TURNS))
        secrets.get.assert_called_once_with("openai-key")
        assert calls["client"] == {"api_key": "sk-secret"}

    def test_empty_reply_is_empty_not_none(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        _fake_openai(monkeypatch, reply=None)
        assert asyncio.run(openai_summarizer(api_key="k")(None, TURNS)) == ""

    def test_missing_sdk_is_named(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import openai_summarizer

        monkeypatch.setitem(sys.modules, "openai", None)
        with pytest.raises(ImportError, match="pip install openai"):
            openai_summarizer()


class TestAnthropicSummarizer:
    def test_key_resolution_prefers_explicit_then_env(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import anthropic_summarizer

        built = {}

        class AsyncAnthropic:
            def __init__(self, **kwargs):
                built.update(kwargs)
                self.messages = types.SimpleNamespace(create=self._create)

            async def _create(self, **kwargs):
                built["create"] = kwargs
                return types.SimpleNamespace(
                    content=[types.SimpleNamespace(type="text", text=" ok ")]
                )

        module = types.ModuleType("anthropic")
        module.AsyncAnthropic = AsyncAnthropic
        monkeypatch.setitem(sys.modules, "anthropic", module)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "env-key")

        assert (
            asyncio.run(anthropic_summarizer(api_key="explicit")(None, TURNS)) == "ok"
        )
        assert built["api_key"] == "explicit"
        assert built["create"]["messages"][0]["role"] == "user"

        asyncio.run(anthropic_summarizer()(None, TURNS))
        assert built["api_key"] == "env-key"


def test_both_summarizers_are_exported():
    import hopsworks_agents.protocol as protocol

    assert callable(protocol.openai_summarizer)
    assert callable(protocol.anthropic_summarizer)


class TestHopsworksSummarizer:
    """Resolved once; the gateway's key replaces the SDK's; the model is discovered."""

    def _fake_openai(self, monkeypatch, models=("llama-3.1-8b",)):
        calls = _fake_openai(monkeypatch)
        AsyncOpenAI = sys.modules["openai"].AsyncOpenAI

        async def list_models():
            return types.SimpleNamespace(
                data=[types.SimpleNamespace(id=m) for m in models]
            )

        original_init = AsyncOpenAI.__init__

        def init(self, **kwargs):
            original_init(self, **kwargs)
            self.models = types.SimpleNamespace(list=list_models)

        monkeypatch.setattr(AsyncOpenAI, "__init__", init)
        httpx = types.ModuleType("httpx")
        httpx.AsyncClient = lambda **kw: ("httpx", kw)
        monkeypatch.setitem(sys.modules, "httpx", httpx)
        return calls

    def _resolved(self, monkeypatch, verify=False):
        import hopsworks_agents.protocol.summarizers as mod

        seen = []

        def resolve(deployment):
            seen.append(deployment)
            return "https://gw/v1/g1/my-llm/v1", "serving-key", verify

        monkeypatch.setattr(mod, "_resolve_hopsworks_deployment", resolve)
        return seen

    def test_goes_through_the_gateway_with_its_key_and_the_served_model(
        self, monkeypatch
    ):
        from hopsworks_agents.protocol.summarizers import hopsworks_summarizer

        calls = self._fake_openai(monkeypatch)
        seen = self._resolved(monkeypatch)
        summarize = hopsworks_summarizer("my-llm")

        assert asyncio.run(summarize(None, TURNS)) == "folded"
        assert asyncio.run(summarize("s", TURNS)) == "folded"
        assert seen == ["my-llm"]  # resolved once
        assert calls["client"] == {
            "base_url": "https://gw/v1/g1/my-llm/v1",
            "api_key": "hopsworks",
            "default_headers": {"Authorization": "ApiKey serving-key"},
            "http_client": ("httpx", {"verify": False}),
        }
        assert calls["create"]["model"] == "llama-3.1-8b"

    def test_an_explicit_model_skips_discovery(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import hopsworks_summarizer

        calls = self._fake_openai(monkeypatch, models=())
        self._resolved(monkeypatch)
        asyncio.run(hopsworks_summarizer("my-llm", "mistral-7b")(None, TURNS))
        assert calls["create"]["model"] == "mistral-7b"

    def test_no_models_listed_is_an_error(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import hopsworks_summarizer

        self._fake_openai(monkeypatch, models=())
        self._resolved(monkeypatch)
        with pytest.raises(ValueError, match="pass model="):
            asyncio.run(hopsworks_summarizer("my-llm")(None, TURNS))

    def test_extra_headers_and_client_options_are_kept(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import hopsworks_summarizer

        calls = self._fake_openai(monkeypatch)
        self._resolved(monkeypatch, verify="/certs/ca.pem")
        summarize = hopsworks_summarizer(
            "my-llm", default_headers={"X-Team": "ml"}, timeout=30
        )
        asyncio.run(summarize(None, TURNS))
        assert calls["client"]["default_headers"] == {
            "Authorization": "ApiKey serving-key",
            "X-Team": "ml",
        }
        assert calls["client"]["timeout"] == 30
        assert calls["client"]["http_client"] == ("httpx", {"verify": "/certs/ca.pem"})

    def test_resolution_reads_the_serving_api_and_the_gateway_client(self, monkeypatch):
        from hopsworks_agents.protocol.summarizers import (
            _resolve_hopsworks_deployment,
        )

        deployment = mock.MagicMock(name="deployment")
        deployment.name = "my-llm"
        deployment.get_openai_url.return_value = "https://gw/v1/g1/my-llm/v1"
        serving = mock.MagicMock()
        serving.get_deployment.side_effect = lambda n: (
            deployment if n == "my-llm" else None
        )
        hopsworks = types.ModuleType("hopsworks")
        hopsworks.login = lambda: types.SimpleNamespace(
            get_model_serving=lambda: serving
        )
        monkeypatch.setitem(sys.modules, "hopsworks", hopsworks)
        from hopsworks_common.client import istio

        gateway = types.SimpleNamespace(
            _auth=types.SimpleNamespace(_token="serving-key"), _verify=False
        )
        monkeypatch.setattr(istio, "_get_instance", lambda: gateway)

        assert _resolve_hopsworks_deployment("my-llm") == (
            "https://gw/v1/g1/my-llm/v1",
            "serving-key",
            False,
        )
        # a Deployment object is used as is
        assert _resolve_hopsworks_deployment(deployment)[0].endswith("/v1")
        with pytest.raises(ValueError, match="No deployment named"):
            _resolve_hopsworks_deployment("other")
        deployment.get_openai_url.return_value = None
        with pytest.raises(ValueError, match="no OpenAI-compatible endpoint"):
            _resolve_hopsworks_deployment("my-llm")


def test_all_three_summarizers_are_exported():
    import hopsworks_agents.protocol as protocol

    assert callable(protocol.hopsworks_summarizer)
