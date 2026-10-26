"""AG-UI on the wire: a run's input becomes a turn, a turn becomes AG-UI events.

``AgentApp`` serves ``POST /v1/run`` in the AG-UI protocol (https://docs.ag-ui.com),
so any AG-UI client can talk to a Hopsworks agent.
The turn pipeline behind it is the one ``/v1/chat`` uses; this module is only the
translation at its two ends.

What Hopsworks adds rides under a ``hopsworks`` key in the extension points AG-UI
defines, so a client that knows nothing of it ignores it:

- in: ``forwardedProps.hopsworks`` carries ``subject``, ``context`` and ``metadata``;
- out: ``RUN_STARTED.metadata.hopsworks.trace_id``, ``RUN_FINISHED.result.hopsworks``
  (trace id, subject, citations), ``RUN_ERROR.metadata.hopsworks`` (the HTTP status
  the error would have had, and whether a retry is worth it), step events'
  ``metadata.hopsworks``, and a ``CUSTOM`` event named ``hopsworks.media`` per image,
  file or audio part of the reply, which AG-UI assistant messages cannot carry.
"""

from __future__ import annotations

from typing import Any

from ag_ui.core import (
    CustomEvent,
    RunAgentInput,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    StepFinishedEvent,
    StepStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    TokenUsage,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
)
from ag_ui.encoder import EventEncoder

from .models import (
    AgentError,
    AudioContent,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    FileContent,
    ImageContent,
    TextContent,
    new_message_id,
)


HOPSWORKS = "hopsworks"
MEDIA_EVENT = "hopsworks.media"

# Roles an earlier turn keeps when it is handed to a framework as history.
_HISTORY_ROLES = {"user": "user", "assistant": "assistant", "system": "system"}
_HISTORY_ROLES["developer"] = "system"


def _to_chat_request(run: RunAgentInput, *, server_history: bool) -> ChatRequest:
    """The turn an AG-UI run asks for, as the request object handlers receive.

    The new turn is the user messages after the last assistant message: a client
    may resend the whole conversation, as AG-UI clients do, or send only the new
    message, as the Hopsworks clients do, and the turn is the same.
    With ``server_history`` the earlier messages are ignored, because the agent's
    memory holds the conversation and a client must not be able to rewrite it.
    Without it they become ``prior_messages``, the history a stateless agent has.

    Raises:
        AgentError: The run carries no user message after the last assistant
            message, or content this agent cannot take.
    """
    messages = list(run.messages or [])
    last_assistant = -1
    for index, message in enumerate(messages):
        if message.role == "assistant":
            last_assistant = index
    new = [m for m in messages[last_assistant + 1 :] if m.role == "user"]
    if not new:
        raise AgentError(
            "The run has no user message after the last assistant message, "
            "so there is nothing to answer.",
            "no_user_message",
            400,
        )
    content: list[Any] = []
    for message in new:
        content.extend(_parts(message.content))
    hopsworks = _hopsworks_props(run.forwarded_props)
    context: dict[str, Any] = {c.description: c.value for c in run.context or []}
    if isinstance(hopsworks.get("context"), dict):
        context.update(hopsworks["context"])
    prior: list[dict[str, str]] = []
    if not server_history:
        for message in messages[: last_assistant + 1]:
            role = _HISTORY_ROLES.get(message.role)
            text = _text_of(getattr(message, "content", None))
            if role and text:
                prior.append({"role": role, "content": text})
    subject = hopsworks.get("subject")
    metadata = hopsworks.get("metadata")
    return ChatRequest(
        conversation_id=run.thread_id or None,
        message=ChatMessage(id=new[-1].id, role="user", content=content),
        subject=subject if isinstance(subject, str) and subject.strip() else None,
        context=context,
        metadata=metadata if isinstance(metadata, dict) else {},
        run_id=run.run_id or None,
        messages=messages,
        tools=list(run.tools or []),
        state=run.state,
        forwarded_props=run.forwarded_props,
        prior_messages=prior,
    )


def _hopsworks_props(forwarded: Any) -> dict[str, Any]:
    if isinstance(forwarded, dict) and isinstance(forwarded.get(HOPSWORKS), dict):
        return forwarded[HOPSWORKS]
    return {}


def _text_of(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            p.text for p in content if getattr(p, "type", None) == "text" and p.text
        )
    return ""


def _parts(content: Any) -> list[Any]:
    """A user message's content as the protocol's typed parts."""
    if isinstance(content, str):
        return [TextContent(text=content)] if content else []
    parts: list[Any] = []
    for part in content or []:
        if part.type == "text":
            parts.append(TextContent(text=part.text))
            continue
        source = getattr(part, "source", None)
        if getattr(source, "type", None) != "data":
            # Fetching a URL or resolving a provider's file handle would mean the
            # agent reaching out on a client's say-so; inline bytes are what the
            # manifest's input modalities promise.
            raise AgentError(
                f"A {part.type} part must carry its bytes inline (source type "
                f"'data'); '{getattr(source, 'type', None)}' sources are not "
                "supported.",
                "unsupported_content",
                422,
            )
        if part.type == "image":
            parts.append(ImageContent(media_type=source.mime_type, data=source.value))
        elif part.type == "audio":
            parts.append(AudioContent(media_type=source.mime_type, data=source.value))
        elif part.type == "document":
            metadata = part.metadata if isinstance(part.metadata, dict) else {}
            name = metadata.get("name") or metadata.get("filename")
            parts.append(
                FileContent(
                    name=name if isinstance(name, str) else None,
                    media_type=source.mime_type,
                    data=source.value,
                )
            )
        else:
            raise AgentError(
                f"{part.type} parts are not supported by this agent.",
                "unsupported_content",
                422,
            )
    return parts


class RunStream:
    """One run's events, encoded for the wire, in the order AG-UI requires.

    Keeps what the order depends on: whether an assistant text message is open,
    which tool calls and steps have started.
    A text message is closed before a tool call or step starts, so the two never
    interleave inside one message.
    """

    def __init__(self, thread_id: str, run_id: str, accept: str | None = None):
        self.thread_id = thread_id
        self.run_id = run_id
        self._encoder = EventEncoder(accept=accept)
        self._text_id: str | None = None
        self._streamed_text = False
        self._started_tools: set[str] = set()
        self._ended_tools: set[str] = set()
        self._args_sent: set[str] = set()
        self._open_steps: set[str] = set()

    @property
    def media_type(self) -> str:
        """The content type the events are encoded in."""
        return self._encoder.get_content_type()

    def _encode(self, *events: Any) -> str:
        return "".join(self._encoder.encode(event) for event in events)

    def _started(self, trace_id: str | None) -> str:
        """``RUN_STARTED``, carrying the trace id so a failed run can still be found."""
        metadata = {HOPSWORKS: {"trace_id": trace_id}} if trace_id else None
        return self._encode(
            RunStartedEvent(
                thread_id=self.thread_id, run_id=self.run_id, metadata=metadata
            )
        )

    def _text(self, delta: str) -> str:
        """A piece of the assistant's answer."""
        if not delta:
            return ""
        out = ""
        if self._text_id is None:
            self._text_id = new_message_id()
            out += self._encode(
                TextMessageStartEvent(message_id=self._text_id, role="assistant")
            )
        self._streamed_text = True
        return out + self._encode(
            TextMessageContentEvent(message_id=self._text_id, delta=delta)
        )

    def _close_text(self) -> str:
        if self._text_id is None:
            return ""
        message_id, self._text_id = self._text_id, None
        return self._encode(TextMessageEndEvent(message_id=message_id))

    def _progress(self, payload: dict[str, Any]) -> str:
        """A progress event from the turn: a tool call, or a step the agent reports."""
        out = self._close_text()
        if payload.get("kind") == "tool":
            return out + self._tool_call(payload)
        return out + self._step(payload)

    def _tool_call(self, payload: dict[str, Any]) -> str:
        call_id = str(payload.get("id") or new_message_id())
        name = str(payload.get("name") or "tool")
        status = payload.get("status")
        events: list[Any] = []
        if call_id not in self._started_tools:
            self._started_tools.add(call_id)
            events.append(ToolCallStartEvent(tool_call_id=call_id, tool_call_name=name))
        # The arguments go out once, whenever they are first known: a framework
        # span usually carries them only by the time it closes.
        if (
            payload.get("args")
            and call_id not in self._args_sent
            and call_id not in self._ended_tools
        ):
            self._args_sent.add(call_id)
            events.append(
                ToolCallArgsEvent(tool_call_id=call_id, delta=payload["args"])
            )
        if status != "running" and call_id not in self._ended_tools:
            self._ended_tools.add(call_id)
            events.append(ToolCallEndEvent(tool_call_id=call_id))
            failed = status == "failed"
            events.append(
                ToolCallResultEvent(
                    message_id=new_message_id(),
                    tool_call_id=call_id,
                    role="tool",
                    content=str(
                        (payload.get("message") or "failed")
                        if failed
                        else (payload.get("result") or "")
                    ),
                    metadata={HOPSWORKS: {"status": status}} if failed else None,
                )
            )
        return self._encode(*events)

    def _step(self, payload: dict[str, Any]) -> str:
        name = str(payload.get("name") or "step")
        status = payload.get("status")
        key = str(payload.get("id") or name)
        detail = {
            k: payload[k]
            for k in ("id", "status", "message", "data")
            if payload.get(k) is not None
        }
        metadata = {HOPSWORKS: detail}
        events: list[Any] = []
        if status == "running":
            self._open_steps.add(key)
            events.append(StepStartedEvent(step_name=name, metadata=metadata))
        else:
            if key not in self._open_steps:
                events.append(StepStartedEvent(step_name=name, metadata=metadata))
            self._open_steps.discard(key)
            events.append(StepFinishedEvent(step_name=name, metadata=metadata))
        return self._encode(*events)

    def _finished(
        self,
        response: ChatResponse,
        *,
        subject: str | None,
        subject_source: str | None,
    ) -> str:
        """The rest of the answer, its media, and ``RUN_FINISHED`` with the turn's facts.

        A turn the handler reported as failed ends in ``RUN_ERROR`` instead, after
        whatever text it produced.
        """
        out = ""
        text = "".join(
            p.text for p in response.message.content if p.type == "text" and p.text
        )
        if text and not self._streamed_text:
            message_id = response.message.id or new_message_id()
            out += self._encode(
                TextMessageStartEvent(message_id=message_id, role="assistant"),
                TextMessageContentEvent(message_id=message_id, delta=text),
                TextMessageEndEvent(message_id=message_id),
            )
        out += self._close_text()
        for part in response.message.content:
            if part.type != "text":
                out += self._encode(
                    CustomEvent(name=MEDIA_EVENT, value=part.model_dump())
                )
        metadata = dict(response.metadata or {})
        trace_id = metadata.pop("trace_id", None)
        metadata.pop("tool_events", None)
        metadata.pop("subject", None)
        if response.status == "failed":
            return out + self._error(
                AgentError(
                    "The agent reported the turn as failed.", "turn_failed", 500
                ),
                trace_id=trace_id,
            )
        facts: dict[str, Any] = {
            "trace_id": trace_id,
            "response_id": response.id,
            "subject": subject,
            "subject_source": subject_source,
            "citations": response.citations,
        }
        if metadata:
            facts["metadata"] = metadata
        return out + self._encode(
            RunFinishedEvent(
                thread_id=self.thread_id,
                run_id=self.run_id,
                result={HOPSWORKS: {k: v for k, v in facts.items() if v is not None}},
                usage=_usage(response.usage),
            )
        )

    def _error(self, err: AgentError, *, trace_id: str | None = None) -> str:
        """``RUN_ERROR``, after closing what was open.

        The status the error would have had over plain HTTP rides in the metadata:
        a 4xx is the agent declining, a 5xx the agent breaking, and the evaluation
        runner tells an unanswered task from a failed one by it.
        """
        detail: dict[str, Any] = {
            "status": err.status_code,
            "retryable": err.retryable,
        }
        if trace_id:
            detail["trace_id"] = trace_id
        return self._close_text() + self._encode(
            RunErrorEvent(
                message=err.message,
                code=err.code,
                metadata={HOPSWORKS: detail},
            )
        )


def _usage(usage: dict[str, int] | None) -> list[TokenUsage] | None:
    """The turn's token counts in AG-UI's accounting, or nothing when none were reported."""
    if not usage:
        return None
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens"))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens"))
    counts = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": usage.get("total_tokens"),
    }
    counts = {k: v for k, v in counts.items() if isinstance(v, int) and v >= 0}
    if not counts:
        return None
    return [TokenUsage(**counts)]
