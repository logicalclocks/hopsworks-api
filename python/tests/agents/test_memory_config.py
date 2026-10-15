"""The deployment's memory policy, and the rule that code beats it."""

import json
import logging
from unittest import mock

import pytest
from hopsworks_agents.protocol import AgentApp
from hopsworks_agents.protocol.memory_config import ENV_VAR, memory_from_env, policy


def env(config) -> dict[str, str]:
    """An environment carrying `config` as the deployment's policy."""
    return {ENV_VAR: config if isinstance(config, str) else json.dumps(config)}


class TestPolicy:
    def test_absent_or_empty_is_no_policy(self):
        assert policy({}) is None
        assert policy({ENV_VAR: "   "}) is None

    def test_a_policy_without_enabled_is_on(self):
        # writing a policy at all is asking for memory; `enabled: false` is how
        # the form switches it back off
        assert policy(env({"long_term": True})) == {"long_term": True}

    def test_enabled_false_is_off(self):
        assert policy(env({"enabled": False, "long_term": True})) is None

    def test_bare_true_is_shorthand_for_on(self):
        assert policy(env(True)) == {"enabled": True}

    @pytest.mark.parametrize("raw", ["{not json", '"a string"', "[1, 2]"])
    def test_unreadable_policies_leave_memory_off_rather_than_crashing(self, raw):
        assert policy({ENV_VAR: raw}) is None

    def test_unknown_keys_are_kept_not_refused(self, caplog):
        # a newer backend may send keys this SDK has never heard of
        caplog.set_level(logging.INFO)
        parsed = policy(env({"long_term": True, "telepathy": 3}))
        assert parsed["long_term"] is True
        assert "telepathy" in caplog.text


class TestMemoryFromEnv:
    def test_no_policy_builds_nothing(self):
        assert memory_from_env({}) is None

    def test_the_tiers_the_policy_asks_for(self):
        with mock.patch(
            "hopsworks_agents.protocol.memory.ManagedMemoryService"
        ) as service:
            memory_from_env(
                env(
                    {
                        "long_term": True,
                        "summarize_after_messages": 6,
                        "keep_recent_messages": 4,
                        "message_retention_days": 30,
                    }
                )
            )
        kwargs = service.call_args.kwargs
        assert kwargs["long_term"] is True
        assert kwargs["summarize_after_messages"] == 6
        assert kwargs["keep_recent_messages"] == 4
        assert kwargs["message_retention_days"] == 30
        assert "summarize" not in kwargs

    def test_a_hopsworks_summarizer_needs_no_key(self):
        with (
            mock.patch(
                "hopsworks_agents.protocol.summarizers.hopsworks_summarizer"
            ) as factory,
            mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService") as svc,
        ):
            memory_from_env(
                env({"summarize": {"provider": "hopsworks", "deployment": "my-llm"}})
            )
        factory.assert_called_once_with("my-llm", None)
        assert svc.call_args.kwargs["summarize"] is factory.return_value

    def test_a_hopsworks_summarizer_without_a_deployment_is_skipped(self, caplog):
        with mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService") as svc:
            memory_from_env(env({"summarize": {"provider": "hopsworks"}}))
        assert "summarize" not in svc.call_args.kwargs
        assert "needs 'deployment'" in caplog.text

    def test_an_openai_summarizer_carries_its_model_and_secret(self):
        with (
            mock.patch(
                "hopsworks_agents.protocol.summarizers.openai_summarizer"
            ) as factory,
            mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService"),
        ):
            memory_from_env(
                env(
                    {
                        "summarize": {
                            "provider": "openai",
                            "model": "gpt-5.6-terra",
                            "api_key_secret": "OPENAI_API_KEY",
                        }
                    }
                )
            )
        assert factory.call_args.args == ("gpt-5.6-terra",)
        assert factory.call_args.kwargs["api_key_secret"] == "OPENAI_API_KEY"

    def test_an_unknown_provider_leaves_the_rest_of_memory_standing(self, caplog):
        with mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService") as svc:
            store = memory_from_env(
                env({"summarize": {"provider": "oracle-of-delphi"}, "long_term": True})
            )
        assert store is svc.return_value
        assert "summarize" not in svc.call_args.kwargs
        assert svc.call_args.kwargs["long_term"] is True
        assert "unknown summarizer provider" in caplog.text

    def test_search_implies_long_term_memory(self, caplog):
        caplog.set_level(logging.INFO)
        with (
            mock.patch(
                "hopsworks_agents.protocol.summarizers.sentence_transformer_embedder"
            ) as embedder,
            mock.patch(
                "hopsworks_agents.protocol.vectorstore.vector_store_for"
            ) as store_for,
            mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService") as svc,
        ):
            memory_from_env(env({"search": {"model": "all-MiniLM-L6-v2"}}))
        embedder.assert_called_once_with("all-MiniLM-L6-v2")
        kwargs = svc.call_args.kwargs
        assert kwargs["long_term"] is True
        assert kwargs["vector_store"] is store_for.return_value
        assert "search implies long-term memory" in caplog.text

    def test_an_embedding_model_that_will_not_load_drops_only_search(self, caplog):
        with (
            mock.patch(
                "hopsworks_agents.protocol.summarizers.sentence_transformer_embedder",
                side_effect=RuntimeError("no model registered"),
            ),
            mock.patch("hopsworks_agents.protocol.memory.ManagedMemoryService") as svc,
        ):
            store = memory_from_env(env({"long_term": True, "search": True}))
        assert store is svc.return_value
        assert "vector_store" not in svc.call_args.kwargs
        assert "falls back to keywords" in caplog.text

    def test_a_store_that_cannot_be_built_does_not_stop_the_agent(self, caplog):
        with mock.patch(
            "hopsworks_agents.protocol.memory.ManagedMemoryService",
            side_effect=RuntimeError("no database"),
        ):
            assert memory_from_env(env({"long_term": True})) is None
        assert "runs without it" in caplog.text


class TestCodeWins:
    def test_an_explicit_store_beats_the_policy(self):
        mine = mock.MagicMock()
        with mock.patch("hopsworks_agents.protocol.app.memory_from_env") as from_env:
            app = AgentApp(memory=mine)
        from_env.assert_not_called()
        assert app.memory is mine
        assert app._memory_capabilities()["source"] == "code"

    def test_an_explicit_none_stays_off(self):
        with mock.patch("hopsworks_agents.protocol.app.memory_from_env") as from_env:
            app = AgentApp(memory=None)
        from_env.assert_not_called()
        assert app.memory is None
        assert app._memory_capabilities()["source"] is None

    def test_saying_nothing_takes_the_deployment_policy(self):
        configured = mock.MagicMock(
            _summarize=None, _long_term=True, _vector_store=None
        )
        with mock.patch(
            "hopsworks_agents.protocol.app.memory_from_env", return_value=configured
        ):
            app = AgentApp()
        assert app.memory is configured
        caps = app._memory_capabilities()
        assert caps["source"] == "deployment"
        assert caps["state"] is True
        # the manifest tells a client where the policy came from
        assert app._manifest()["capabilities"]["memory"]["source"] == "deployment"
