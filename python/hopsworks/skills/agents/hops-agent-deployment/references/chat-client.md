# Talking to an agent from code: details

`agent.chat()`, `chat_stream()` and the chat app are in SKILL.md, "Talk to the agent".

**What `manifest()` returns** is the protocol document, not a flat summary: the agent's own fields are under `agent` (`name`, `description`, `version`, `framework`), the routes under `endpoints`, the UI hints under `ui`, and `capabilities` is AG-UI's `AgentCapabilities` with what only Hopsworks reads (streaming, memory tiers, trace correlation, evaluation mode) under `capabilities["custom"]["hopsworks"]`. `manifest()["agent"]["framework"]`, not `manifest()["framework"]` — the latter is `None` for every agent.

**Tool events pair by id.** Each tool call yields more than one event, `status` going `running` then `done`, matched on the id. The closing event is named for the event, not for the tool — an agent that emits `ctx.emit_event("tool", status="done")` ends with `name == "tool"` — so follow the id rather than the name.

They arrive in two shapes, which is worth knowing before writing the loop. `stream.tool_events` are `ToolEvent` **objects** — `.id`, `.name`, `.status`, `.message`, `.data`, `.raw`; the field is `id`, not `event_id`, and `json.dumps(event)` raises. On a non-streamed `agent.chat()` the same events ride on `reply.metadata["tool_events"]` as plain dicts with the same keys:

```python
for event in stream.tool_events:
    print(event.id, event.name, event.status)                  # objects
for event in reply.metadata.get("tool_events", []):
    print(event["id"], event["name"], event["status"])         # dicts, same fields
```

**Don't emit your own when `tool_events=True`.** The flag bridges the framework's tool spans to events by itself, which is the point of it: no code in the agent. An agent that also calls `ctx.emit_event` for the same tool used to produce two chips per call under two id spaces — the framework's call id and the span id. The automatic emitter now stays off any tool the handler reports itself, so either style works and mixing them is harmless; a tool you never claim is still reported for you.

**Reading a conversation back.** `agent.traces()` lists conversations, one row per session; `agent.session(session_id)` gives that conversation's turns and `agent.conversation(session_id)` pairs them as `{trace_id, user, assistant}` — `role`/`content` is the shape of the messages inside a turn, not of these.

**A custom chat UI** talks to `agent.url` directly with the serving credential: `POST /v1/chat`
with `{"message": {"role": "user", "content": [{"type": "text", "text": "..."}]}}`, and its users
rate a reply with `POST /v1/feedback` `{"trace_id": reply.metadata.trace_id, "verdict":
"negative", "subject": "<who>"}`. The agent relays the verdict to Hopsworks itself, filed under
`user:<subject>`; the end user needs no Hopsworks account and the UI never learns the Hopsworks
address. See "End-user feedback" in `python/hopsworks_agents/protocol/README.md`.
