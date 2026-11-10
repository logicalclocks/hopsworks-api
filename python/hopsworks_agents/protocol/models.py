"""Pydantic models for the turn an agent answers.

A turn arrives as an AG-UI run on /v1/run (see ``agui``) or as a ChatRequest on
/v1/chat, and handlers see a ChatRequest either way; they answer with a
ChatResponse. History is server-side via conversation_id when memory is on.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from hopsworks_apigen import public
from pydantic import BaseModel, Field


# The wire protocol, and the version of the Hopsworks extensions carried on it.
PROTOCOL = "ag-ui"
PROTOCOL_VERSION = "2.0"


@public
def new_conversation_id() -> str:
    return f"conv_{uuid.uuid4().hex}"


@public
def new_message_id() -> str:
    return f"msg_{uuid.uuid4().hex}"


@public
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


class ResumeAnswer(BaseModel):
    """A person's answer to an interrupt: what ``ctx.interrupt`` returns on the run that resumes it.

    ``status`` is ``resolved`` when the person answered, with what they said in
    ``payload``, and ``cancelled`` when they declined to answer. A refusal to go
    ahead is an answer, so it is ``resolved`` with a payload that says so, for
    example ``{"approved": False}``.
    """

    interrupt_id: str
    status: Literal["resolved", "cancelled"]
    payload: Any = None
    metadata: dict[str, Any] | None = None
    #: A fingerprint of the question this answers (its message, reason, schema, tool call
    #: and metadata), set by the agent when it replays the paused turn: the answer is
    #: taken only when the replay asks that same question, and a question that changed
    #: is asked again. None for an answer that did not come through a replay.
    asked: str | None = None

    @property
    def resolved(self) -> bool:
        """Whether the person answered."""
        return self.status == "resolved"

    @property
    def cancelled(self) -> bool:
        """Whether the person declined to answer."""
        return self.status == "cancelled"


class ChatRequest(BaseModel):
    conversation_id: str | None = None
    # Required, except on a request that resumes an interrupted turn: that one
    # replays the message the turn was asked with, which the agent kept.
    message: ChatMessage | None = None
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
    # What an AG-UI run sent beyond the new message, for a handler that wants it.
    # Empty for a turn that arrived on /v1/chat.
    run_id: str | None = None
    messages: list[Any] = Field(default_factory=list)
    tools: list[Any] = Field(default_factory=list)
    state: Any = None
    forwarded_props: Any = None
    # The turns before this one, as the client sent them, for an agent with no
    # memory. With memory the server's transcript is the history and this stays
    # empty, so a client resending the conversation cannot rewrite it.
    prior_messages: list[dict[str, str]] = Field(default_factory=list)
    # The answers to the interrupts that paused this turn's previous run. When
    # present the turn is that one again, replayed: ``ctx.interrupt`` returns
    # the answer instead of pausing.
    resume: list[ResumeAnswer] = Field(default_factory=list)

    @property
    def text(self) -> str:
        """The user message as plain text (text parts joined)."""
        if self.message is None:
            return ""
        return "\n".join(
            part.text for part in self.message.content if part.type == "text"
        ).strip()

    @property
    def _parts(self) -> list[Any]:
        return self.message.content if self.message is not None else []

    @property
    def images(self) -> list[ImageContent]:
        return [p for p in self._parts if p.type == "image"]

    @property
    def files(self) -> list[FileContent]:
        return [p for p in self._parts if p.type == "file"]

    @property
    def audio_clips(self) -> list[AudioContent]:
        return [p for p in self._parts if p.type == "audio"]

    @public
    def to_framework_messages(self) -> list[dict[str, str]]:
        """The new message in the `{"role", "content"}` dict shape that.

        LangChain, LangGraph, and LlamaIndex all accept directly.

        With memory configured the history is server-side, so this is only the
        current turn; pair it with your framework's checkpointer/memory keyed by
        ``conversation_id``. Without memory, an AG-UI client sends the
        conversation, and the earlier turns come first.
        """
        return [
            *self.prior_messages,
            {
                "role": self.message.role if self.message else "user",
                "content": self.text,
            },
        ]


class FeedbackRequest(BaseModel):
    """An end user's verdict on a turn, posted to the agent's own ``/v1/feedback``.

    Names the turn by ``trace_id`` (from the reply's metadata) or, when a client
    never kept it, by ``conversation_id``, which resolves to that conversation's
    latest turn. ``subject`` is who is speaking, client-asserted like the chat
    request's; it is what the feedback is filed under. ``source`` is where the
    verdict was given, a short tag such as ``chat-app``, so a reviewer can tell
    one place's verdicts from another's.
    """

    trace_id: str | None = None
    conversation_id: str | None = None
    verdict: Literal["positive", "negative", "false_alarm"]
    issue_category: str | None = None
    corrected_answer: str | None = None
    expected_tool_behavior: str | None = None
    note: str | None = None
    subject: str | None = None
    source: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9_-]{0,31}$")


class ChatResponse(BaseModel):
    id: str = Field(default_factory=new_response_id)
    conversation_id: str
    message: ChatMessage
    citations: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, int] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    # "interrupted" when the agent paused to ask a person: ``interrupts`` says
    # what it asked, and the turn goes on when a request carrying ``resume``
    # answers every one of them.
    status: Literal["completed", "failed", "interrupted"] = "completed"
    interrupts: list[dict[str, Any]] = Field(default_factory=list)


class AgentResponse:
    """Ergonomic constructors for ChatResponse."""

    @staticmethod
    @public
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
    @public
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

    @public
    def detail(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
