#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""The memory a deployment gives an agent, as the SDK takes it."""

import pytest
from hsml.deployment_memory_config import DeploymentMemoryConfig


class TestToDict:
    def test_every_tier_is_on_unless_it_is_turned_off(self):
        # the summarizer is the exception: nothing folds without a model
        assert DeploymentMemoryConfig().to_dict() == {
            "enabled": True,
            "long_term": True,
            "search": {},
        }

    def test_a_tier_left_out_by_saying_so(self):
        assert DeploymentMemoryConfig(long_term=False, search=False).to_dict() == {
            "enabled": True,
            "long_term": False,
        }

    def test_the_keys_are_the_agent_sdk_s_own(self):
        # snake_case, because the agent reads this as AGENT_MEMORY_CONFIG rather
        # than through the platform's camelCase API
        config = DeploymentMemoryConfig(
            summarize={"provider": "hopsworks", "deployment": "my-llm"},
            long_term=True,
            search={"model": "all-MiniLM-L6-v2"},
            summarize_after_messages=6,
            keep_recent_messages=4,
            message_retention_days=30,
        )
        assert config.to_dict() == {
            "enabled": True,
            "summarize": {"provider": "hopsworks", "deployment": "my-llm"},
            "long_term": True,
            "search": {"model": "all-MiniLM-L6-v2"},
            "summarize_after_messages": 6,
            "keep_recent_messages": 4,
            "message_retention_days": 30,
        }

    def test_a_bare_provider_name_is_enough(self):
        assert DeploymentMemoryConfig(summarize="openai").to_dict()["summarize"] == {
            "provider": "openai"
        }

    def test_search_true_asks_for_the_default_model(self):
        assert DeploymentMemoryConfig(search=True).to_dict()["search"] == {}

    def test_search_false_is_left_out_entirely(self):
        assert "search" not in DeploymentMemoryConfig(search=False).to_dict()

    def test_a_bare_provider_keeps_the_other_tiers_on(self):
        config = DeploymentMemoryConfig(summarize="anthropic").to_dict()
        assert config["long_term"] is True
        assert config["search"] == {}


class TestValidation:
    def test_an_unknown_provider_is_refused_here_rather_than_in_a_pod(self):
        with pytest.raises(ValueError, match="is not valid"):
            DeploymentMemoryConfig(summarize={"provider": "oracle-of-delphi"})

    def test_a_hopsworks_summarizer_needs_a_deployment_to_fold_on(self):
        with pytest.raises(ValueError, match="deployment"):
            DeploymentMemoryConfig(summarize={"provider": "hopsworks"})

    def test_the_provider_name_is_normalised(self):
        config = DeploymentMemoryConfig(summarize={"provider": "OpenAI"})
        assert config.summarize["provider"] == "openai"


class TestFromJson:
    def test_a_stored_policy_comes_back(self):
        config = DeploymentMemoryConfig.from_response_json(
            {
                "enabled": True,
                "longTerm": True,
                "summarizeAfterMessages": 6,
                "summarize": {"provider": "openai"},
            }
        )
        assert config.long_term is True
        assert config.summarize_after_messages == 6
        assert config.summarize["provider"] == "openai"

    def test_a_round_trip_keeps_the_policy(self):
        config = DeploymentMemoryConfig(
            summarize="anthropic", long_term=True, message_retention_days=7
        )
        assert (
            DeploymentMemoryConfig.from_json(config.to_dict()).to_dict()
            == config.to_dict()
        )
