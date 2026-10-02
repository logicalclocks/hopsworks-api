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
# ruff: noqa: F401 -- the TYPE_CHECKING imports below are for type checkers; runtime loads lazily
"""Server helpers for the Hopsworks Agent Protocol.

``AgentApp`` makes any Python agent chat-ready in the Hopsworks UI; the
memory, vector-store and tool helpers give it durable memory. All of that
needs the ``agents`` extra (FastAPI, pydantic, OpenTelemetry) and runs inside
an agent deployment.

The names are loaded on first use rather than at import, because the same
package also carries what a client needs -- the wire conventions, the
detectors -- and a notebook that only talks to a deployed agent must be able
to import it without a web framework installed.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from .app import AgentApp
    from .context import HandlerContext
    from .embeddings import load_sentence_transformer, register_sentence_transformer
    from .memory import (
        ChatMemory,
        InMemoryAgentMemory,
        ManagedMemoryService,
        deployment_mysql_url,
    )
    from .models import (
        AgentError,
        AgentResponse,
        AudioContent,
        ChatMessage,
        ChatRequest,
        ChatResponse,
        FileContent,
        ImageContent,
        TextContent,
    )
    from .summarizers import (
        anthropic_summarizer,
        hopsworks_summarizer,
        openai_summarizer,
        sentence_transformer_embedder,
    )
    from .tools import (
        forget,
        identify,
        identity_tools,
        memory_tools,
        recall,
        remember,
        search,
    )
    from .vectorstore import (
        HopsworksVectorStore,
        InMemoryVectorStore,
        VectorStore,
        vector_store_for,
    )

# name -> the submodule that defines it
_EXPORTS: dict[str, str] = {
    "AgentApp": "app",
    "HandlerContext": "context",
    "ChatMemory": "memory",
    "InMemoryAgentMemory": "memory",
    "ManagedMemoryService": "memory",
    "deployment_mysql_url": "memory",
    "load_sentence_transformer": "embeddings",
    "register_sentence_transformer": "embeddings",
    "anthropic_summarizer": "summarizers",
    "openai_summarizer": "summarizers",
    "hopsworks_summarizer": "summarizers",
    "sentence_transformer_embedder": "summarizers",
    "HopsworksVectorStore": "vectorstore",
    "vector_store_for": "vectorstore",
    "InMemoryVectorStore": "vectorstore",
    "VectorStore": "vectorstore",
    "forget": "tools",
    "identify": "tools",
    "identity_tools": "tools",
    "memory_tools": "tools",
    "recall": "tools",
    "remember": "tools",
    "search": "tools",
    "AgentError": "models",
    "AgentResponse": "models",
    "AudioContent": "models",
    "ChatMessage": "models",
    "ChatRequest": "models",
    "ChatResponse": "models",
    "FileContent": "models",
    "ImageContent": "models",
    "TextContent": "models",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        return getattr(importlib.import_module(f".{module}", __name__), name)
    except ModuleNotFoundError as err:
        # the client half of the package is installed; the server half is an extra
        raise ModuleNotFoundError(
            f"{err.name} is needed for hopsworks_agents.protocol.{name}; install "
            "hopsworks[agents] to serve an agent"
        ) from err


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
