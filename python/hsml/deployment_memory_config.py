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
from __future__ import annotations

import json
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util


@public
class DeploymentMemoryConfig:
    """Memory an agent deployment is given, for an agent that configures none itself.

    The agent SDK applies this only when the agent passed no memory store of its own, so code
    always wins: an agent that built one keeps it, and one that deliberately passed none stays
    without memory.

    The policy names the tiers: working memory is the conversation kept server-side, short-term
    memory folds older turns into a rolling summary, semantic memory keeps what the agent
    decides to remember across conversations, and episodic memory searches what was said
    before. All of them are on unless you say otherwise, except the summarizer, which has
    nothing to fold with until a provider is named.

    ```python
    ms = project.get_model_serving()

    agent = ms.deploy_agent(
        entry="my_agent.py",
        memory=DeploymentMemoryConfig(
            summarize={"provider": "hopsworks", "deployment": "my-llm"},
            long_term=True,
        ),
    )
    ```

    Parameters:
        enabled: Whether the deployment configures memory at all. Defaults to True.
        summarize:
            Which model folds older turns into the rolling summary, as a dict with a
            ``provider`` of ``hopsworks``, ``openai`` or ``anthropic``.
            The Hopsworks provider names an LLM deployed in the same project with
            ``deployment`` and needs no key; the others take an optional ``model``,
            ``api_key_secret`` and ``base_url``.
            None keeps every turn verbatim.
        long_term: Whether the agent keeps what it decides to remember across conversations.
            This is the store the ``remember`` / ``recall`` / ``forget`` tools write to, scoped
            to the person it is talking to by default, or to one conversation.
            On by default; pass False to leave semantic memory out.
        search:
            Semantic search over the person's past conversations, as a dict naming an
            embedding ``model`` from the project's model registry, or ``{}`` for the SDK's
            default. Implies ``long_term``.
            On by default; pass False to search by keyword instead.
        summarize_after_messages: Messages before older turns are folded.
        keep_recent_messages: Messages left verbatim by a fold. Must be smaller than
            ``summarize_after_messages``.
        message_retention_days: Days a recorded conversation is kept. None keeps it.
    """

    PROVIDER_HOPSWORKS = "hopsworks"
    PROVIDER_OPENAI = "openai"
    PROVIDER_ANTHROPIC = "anthropic"
    VALID_PROVIDERS = (PROVIDER_HOPSWORKS, PROVIDER_OPENAI, PROVIDER_ANTHROPIC)

    def __init__(
        self,
        enabled: bool | None = True,
        summarize: dict[str, Any] | str | None = None,
        long_term: bool | None = True,
        search: dict[str, Any] | bool | None = True,
        summarize_after_messages: int | None = None,
        keep_recent_messages: int | None = None,
        message_retention_days: int | None = None,
        **kwargs,
    ):
        self._enabled = enabled
        self._summarize = self._validate_summarize(summarize)
        self._long_term = long_term
        self._search = self._validate_search(search)
        self._summarize_after_messages = summarize_after_messages
        self._keep_recent_messages = keep_recent_messages
        self._message_retention_days = message_retention_days

    @public
    def describe(self):
        """Print a JSON description of the memory configuration."""
        util.pretty_print(self)

    @classmethod
    def _validate_summarize(cls, summarize):
        """The summarizer as the backend takes it, or None.

        A bare provider name is accepted, since naming the provider is the whole
        configuration for two of the three.
        """
        if summarize is None:
            return None
        if isinstance(summarize, str):
            summarize = {"provider": summarize}
        if not isinstance(summarize, dict):
            raise ValueError(
                "Memory summarizer must be a provider name or a dict, not "
                f"{type(summarize).__name__}"
            )
        provider = (summarize.get("provider") or "").strip().lower()
        if provider not in cls.VALID_PROVIDERS:
            raise ValueError(
                "Memory summarizer provider '{}' is not valid. Possible values are '{}'".format(
                    summarize.get("provider"), ", ".join(cls.VALID_PROVIDERS)
                )
            )
        if (
            provider == cls.PROVIDER_HOPSWORKS
            and not (summarize.get("deployment") or "").strip()
        ):
            raise ValueError(
                "A hopsworks memory summarizer needs the name of the LLM deployment to "
                "fold on, as deployment='<name>'"
            )
        resolved = {k: v for k, v in summarize.items() if v is not None}
        resolved["provider"] = provider
        return resolved

    @classmethod
    def _validate_search(cls, search):
        """Episodic search as the backend takes it: a dict, or None when it is off."""
        if search is None or search is False:
            return None
        if search is True:
            return {}
        if not isinstance(search, dict):
            raise ValueError(
                f"Memory search must be a dict or a bool, not {type(search).__name__}"
            )
        return {k: v for k, v in search.items() if v is not None}

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        fields = cls.extract_fields_from_json(json_decamelized)
        return DeploymentMemoryConfig(**fields) if fields else None

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        config = (
            json_decamelized.pop("memory")
            if "memory" in json_decamelized
            else json_decamelized.pop("memory_config")
            if "memory_config" in json_decamelized
            else json_decamelized
        )
        if not config:
            return {}
        return {
            "enabled": util._extract_field_from_json(config, "enabled"),
            "summarize": util._extract_field_from_json(config, "summarize"),
            "long_term": util._extract_field_from_json(config, "long_term"),
            "search": util._extract_field_from_json(config, "search"),
            "summarize_after_messages": util._extract_field_from_json(
                config, "summarize_after_messages"
            ),
            "keep_recent_messages": util._extract_field_from_json(
                config, "keep_recent_messages"
            ),
            "message_retention_days": util._extract_field_from_json(
                config, "message_retention_days"
            ),
        }

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """The policy in the shape the agent SDK reads.

        snake_case throughout, because these keys are the SDK's own rather than the
        platform's: they reach the agent as the AGENT_MEMORY_CONFIG environment variable.
        """
        config: dict[str, Any] = {
            "enabled": True if self._enabled is None else self._enabled
        }
        if self._summarize is not None:
            config["summarize"] = self._summarize
        if self._long_term is not None:
            config["long_term"] = self._long_term
        if self._search is not None:
            config["search"] = self._search
        if self._summarize_after_messages is not None:
            config["summarize_after_messages"] = self._summarize_after_messages
        if self._keep_recent_messages is not None:
            config["keep_recent_messages"] = self._keep_recent_messages
        if self._message_retention_days is not None:
            config["message_retention_days"] = self._message_retention_days
        return config

    @public
    @property
    def enabled(self):
        """Whether the deployment configures memory at all."""
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool | None):
        self._enabled = enabled

    @public
    @property
    def summarize(self):
        """Which model folds older turns into the rolling summary."""
        return self._summarize

    @summarize.setter
    def summarize(self, summarize: dict[str, Any] | str | None):
        self._summarize = self._validate_summarize(summarize)

    @public
    @property
    def long_term(self):
        """Whether what the agent remembers is kept across conversations."""
        return self._long_term

    @long_term.setter
    def long_term(self, long_term: bool | None):
        self._long_term = long_term

    @public
    @property
    def search(self):
        """Semantic search over the person's past conversations."""
        return self._search

    @search.setter
    def search(self, search: dict[str, Any] | bool | None):
        self._search = self._validate_search(search)

    @public
    @property
    def summarize_after_messages(self):
        """Messages before older turns are folded."""
        return self._summarize_after_messages

    @summarize_after_messages.setter
    def summarize_after_messages(self, summarize_after_messages: int | None):
        self._summarize_after_messages = summarize_after_messages

    @public
    @property
    def keep_recent_messages(self):
        """Messages left verbatim by a fold."""
        return self._keep_recent_messages

    @keep_recent_messages.setter
    def keep_recent_messages(self, keep_recent_messages: int | None):
        self._keep_recent_messages = keep_recent_messages

    @public
    @property
    def message_retention_days(self):
        """Days a recorded conversation is kept."""
        return self._message_retention_days

    @message_retention_days.setter
    def message_retention_days(self, message_retention_days: int | None):
        self._message_retention_days = message_retention_days

    def __repr__(self):
        return f"DeploymentMemoryConfig({self.to_dict()!r})"
