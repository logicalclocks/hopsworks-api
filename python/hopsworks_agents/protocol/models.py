"""Pydantic models for the Hopsworks Agent Protocol v1.0.

Spec: the manifest is served at /.well-known/hopsworks-agent.json; chat is a
ChatRequest -> ChatResponse exchange with typed content parts; history is
server-side via conversation_id.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


PROTOCOL = "hopsworks-agent"
PROTOCOL_VERSION = "1.4"


def new_conversation_id() -> str:
    return f"conv_{uuid.uuid4().hex}"


def new_message_id() -> str:
    return f"msg_{uuid.uuid4().hex}"


def new_response_id() -> str:
    return f"response_{uuid.uuid4().hex}"


class TextContent(BaseModel):
    type: Literal["text"] = "text"
    text: str


class ImageContent(BaseModel):
    type: Literal["image"] = "image"
    media_type: str
    data: str  # base64


class FileContent(BaseModel):
    type: Literal["file"] = "file"
    name: str | None = None
    media_type: str
    data: str  # base64


class AudioContent(BaseModel):
    type: Literal["audio"] = "audio"
    media_type: str
    data: str  # base64


ContentPart = Annotated[
    TextContent | ImageContent | FileContent | AudioContent,
    Field(discriminator="type"),
]


class ChatMessage(BaseModel):
    id: str | None = None
    role: Literal["system", "user", "assistant"]
    content: list[ContentPart]


class ChatRequest(BaseModel):
    conversation_id: str | None = None
    message: ChatMessage
    # End-user identity, used to key durable per-user memory. CLIENT-ASSERTED:
    # the chat transport authenticates with a project-wide serving key, so
    # anyone holding it can claim any subject — and because `subject` keys both
    # state lookup and memory search, claiming another one reads and writes
    # that user's memories. That matches the documented v1 trust boundary
    # (everyone with the key is a project member) and nothing weaker should
    # rely on it. Deriving it server-side from a per-user token is the intended
    # replacement.
    subject: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def text(self) -> str:
        """The user message as plain text (text parts joined)."""
        return "\n".join(
            part.text for part in self.message.content if part.type == "text"
        ).strip()

    @property
    def images(self) -> list[ImageContent]:
        return [p for p in self.message.content if p.type == "image"]

    @property
    def files(self) -> list[FileContent]:
        return [p for p in self.message.content if p.type == "file"]

    @property
    def audio_clips(self) -> list[AudioContent]:
        return [p for p in self.message.content if p.type == "audio"]

    def to_framework_messages(self) -> list[dict[str, str]]:
        """The new message in the `{"role", "content"}` dict shape that.

        LangChain, LangGraph, and LlamaIndex all accept directly.

        History is server-side in the protocol, so this is only the current
        turn; pair it with your framework's checkpointer/memory keyed by
        ``conversation_id``.
        """
        return [{"role": self.message.role, "content": self.text}]


class FeedbackRequest(BaseModel):
    """An end user's verdict on a turn, posted to the agent's own ``/v1/feedback``.

    Names the turn by ``trace_id`` (from the reply's metadata) or, when a client
    never kept it, by ``conversation_id``, which resolves to that conversation's
    latest turn. ``subject`` is who is speaking, client-asserted like the chat
    request's; it is what the feedback is filed under.
    """

    trace_id: str | None = None
    conversation_id: str | None = None
    verdict: Literal["positive", "negative", "false_alarm"]
    issue_category: str | None = None
    corrected_answer: str | None = None
    expected_tool_behavior: str | None = None
    note: str | None = None
    subject: str | None = None


class ChatResponse(BaseModel):
    id: str = Field(default_factory=new_response_id)
    conversation_id: str
    message: ChatMessage
    citations: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, int] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: Literal["completed", "failed"] = "completed"


class AgentResponse:
    """Ergonomic constructors for ChatResponse."""

    @staticmethod
    def text(
        text: str,
        conversation_id: str | None = None,
        citations: list[dict[str, Any]] | None = None,
        usage: dict[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ChatResponse:
        return AgentResponse.parts(
            TextContent(text=text),
            conversation_id=conversation_id,
            citations=citations,
            usage=usage,
            metadata=metadata,
        )

    @staticmethod
    def parts(
        *parts: TextContent | ImageContent | FileContent | AudioContent,
        conversation_id: str | None = None,
        citations: list[dict[str, Any]] | None = None,
        usage: dict[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ChatResponse:
        """Multimodal response: mix text/image/file/audio content parts.

        Only return part types declared in the agent's ``output_modalities``.
        """
        return ChatResponse(
            # a missing conversation_id is filled in by AgentApp from the
            # request before the response is returned
            conversation_id=conversation_id or "",
            message=ChatMessage(
                id=new_message_id(),
                role="assistant",
                content=list(parts),
            ),
            citations=citations or [],
            usage=usage,
            metadata=metadata or {},
        )


class AgentError(Exception):
    """Raise from a handler to return a structured protocol error."""

    def __init__(
        self,
        message: str,
        code: str = "agent_error",
        status_code: int = 500,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status_code = status_code
        self.retryable = retryable

    def detail(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
