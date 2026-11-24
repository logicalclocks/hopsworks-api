"""Pydantic models for the turn an agent answers.

A turn arrives as an AG-UI run on /v1/run (see ``agui``) or as a ChatRequest on
/v1/chat, and handlers see a ChatRequest either way; they answer with a
ChatResponse. History is server-side via conversation_id when memory is on.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from hopsworks_apigen import public
from pydantic import BaseModel, Field, field_validator, model_validator


# The wire protocol, and the version of the Hopsworks extensions carried on it.
PROTOCOL = "ag-ui"
PROTOCOL_VERSION = "2.0"


@public
def new_conversation_id() -> str:
    return f"conv_{uuid.uuid4().hex}"


def _printable(value: str) -> bool:
    """Whether an id carries no control character (the A2A task-key separator among them)."""
    return all(ord(ch) >= 32 and ch != "\x7f" for ch in value)


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


class UrlContent(BaseModel):
    """A part the caller gave by reference rather than inline.

    An A2A `url` part arrives as this, untouched: the agent never fetches a URL
    on a caller's say-so, since that would be a request made with the agent's
    network position and credentials.
    An app that names the hosts it trusts (`AgentApp(fetch_urls=[...])`) has
    those fetched into a [`FileContent`][hopsworks_agents.protocol.models.FileContent]
    before the handler runs; every other URL reaches the handler as is.
    """

    type: Literal["url"] = "url"
    url: str
    media_type: str | None = None
    name: str | None = None


ContentPart = Annotated[
    TextContent | ImageContent | FileContent | AudioContent | UrlContent,
    Field(discriminator="type"),
]

_MEDIA_PARTS = {"image": ImageContent, "audio": AudioContent}


def _part_for(media_type: str, data: str, name: str | None) -> Any:
    """The typed part for inline bytes, by media type: image, audio, or a file."""
    kind = _MEDIA_PARTS.get((media_type or "").split("/", 1)[0])
    if kind is not None:
        return kind(media_type=media_type, data=data)
    return FileContent(name=name, media_type=media_type, data=data)


class Artifact(BaseModel):
    """A named output of a turn, beyond the reply text.

    A2A clients receive each one as a task artifact with the name given here.
    Build one from typed `parts`, or from inline bytes with `data` and
    `media_type`, which become one image, audio or file part by media type.
    `id` is the artifact's stable identity for a client that follows it
    across chunks; it defaults to the name.

    Example:
        ```python
        return AgentResponse.text(
            summary,
            artifacts=[Artifact(name="churn.csv", media_type="text/csv", data=csv_b64)],
        )
        ```
    """

    name: str
    parts: list[ContentPart] = Field(default_factory=list)
    description: str | None = None
    id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _inline_bytes(cls, values: Any) -> Any:
        if isinstance(values, dict) and "data" in values and "parts" not in values:
            values = dict(values)
            data = values.pop("data")
            if isinstance(data, (bytes, bytearray)):
                import base64  # noqa: PLC0415

                data = base64.b64encode(bytes(data)).decode()
            media_type = values.pop("media_type", None) or "application/octet-stream"
            values["parts"] = [_part_for(media_type, data, values.get("name"))]
        return values

    @property
    def artifact_id(self) -> str:
        """The id clients follow the artifact by: the one given, else the name."""
        return self.id or self.name


class AgentSkill(BaseModel):
    """One thing the agent can do, as its A2A Agent Card advertises it.

    An agent that declares none advertises a single `chat` skill built from
    its name, description and suggested prompts.
    `input_modes` and `output_modes` are media types; empty means the agent's
    defaults apply.
    """

    id: str
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    examples: list[str] = Field(default_factory=list)
    input_modes: list[str] = Field(default_factory=list)
    output_modes: list[str] = Field(default_factory=list)


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

    @field_validator("conversation_id")
    @classmethod
    def _no_control_characters(cls, value: str | None) -> str | None:
        # A control character separates a context id from a task id in the key
        # of an A2A task's pending record (pending.TASK_KEY_SEPARATOR): kept out
        # of conversation ids, no conversation can name a task's record.
        if value is not None and not _printable(value):
            raise ValueError("conversation_id may not carry control characters")
        return value

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

    @property
    def urls(self) -> list[UrlContent]:
        """Parts the caller gave by reference, for the handler to fetch or not."""
        return [p for p in self._parts if p.type == "url"]

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
    # Named outputs beyond the reply, for A2A clients; see Artifact. The
    # chat panel does not render them, and AG-UI carries them as custom events.
    artifacts: list[Artifact] = Field(default_factory=list)


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
        artifacts: list[Artifact] | None = None,
    ) -> ChatResponse:
        return AgentResponse.parts(
            TextContent(text=text),
            conversation_id=conversation_id,
            citations=citations,
            usage=usage,
            metadata=metadata,
            artifacts=artifacts,
        )

    @staticmethod
    @public
    def parts(
        *parts: TextContent | ImageContent | FileContent | AudioContent,
        conversation_id: str | None = None,
        citations: list[dict[str, Any]] | None = None,
        usage: dict[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
        artifacts: list[Artifact] | None = None,
    ) -> ChatResponse:
        """Multimodal response: mix text/image/file/audio content parts.

        Only return part types declared in the agent's ``output_modalities``.
        ``artifacts`` are named outputs for A2A clients, next to the reply.
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
            artifacts=list(artifacts or []),
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
