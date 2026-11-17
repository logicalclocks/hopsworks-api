## Who long-term memory belongs to

`user`-scope memory is keyed by the request's **subject**, and the subject is whatever the client asserted — `agent.chat(..., subject="marcus@example.com")`, the chat panel's signed-in user, or your own application calling `ctx.rebind_subject(key)` after it has verified the person. That is what makes a second conversation start knowing them.

**Asking the user who they are does not do this.** The `identify` tool (from `identity_tools`) calls `ctx.record_subject(..., source="model")`, which *labels* the conversation for traces and failure analysis and nothing more. It deliberately does not rebind: rebinding decides whose durable memory a conversation may read, and the caller there is a model acting on text a user typed, so an agent could be talked into reading someone else's facts. With `identify` alone, every `remember(..., scope="user")` is owned by the conversation id, and the next conversation — same email, same person — knows nothing.

So, when you design an agent with long-term memory:

- a real deployment has a login: the app asserts `subject` per request and nobody is asked anything;
- a demo without one either passes `subject=` from the client, or calls `ctx.rebind_subject` in app code after the email is given — and should say in its own README why that is acceptable only for a demo;
- `ctx.has_subject` is true exactly when the turn has a real identity, and `ctx.subject_source` says how it was known: `"client"` (asserted on the request), `"app"` (bound this turn with `rebind_subject`) or `"conversation"` (bound on an earlier turn of this conversation and read back by the SDK — what every turn after the binding reports). The per-conversation fallback is the one case with `has_subject` false. Check `has_subject`, not a list of sources: an agent that only trusted `"client"`/`"app"` never told the model it was identified on turn three, and the model called `identify` again.
- a trace's `subject_source` (the traces table) can also read `"model"`: a label from `identify` on a conversation nobody bound. A model's label never replaces a binding, and `identify` on a bound conversation answers that it is already identified.
- the traces table's subject is the **conversation's**, as it stands now, on every row of it — the first turn of a conversation shows the address given on the second. Which turn identified the person is not readable there; the `identify` call in that turn's `trace.tool_calls` is.
- an **evaluation trial** with a subject reads that person's memory and never writes it: `remember`/`forget` under `user` scope land under the trial's own conversation, and its messages stay out of the person's history. That is what lets a regression replay a customer's turn as them (see **hops-agent-evals**) without the suite changing what is stored about them.

**Reading back what an agent remembers.** From its memory feature groups, so the agent need not be running:

```python
agent.memory_conversations(subject="dana@example.com")   # what it stored, newest first
agent.memory_transcript(conversation_id)                  # messages, summary, and whose it is
agent.memory_facts("dana@example.com")                    # the person's user-scope facts
```

**Searching earlier conversations by meaning.** `agent.memory_search("what do they eat?", subject=..., conversation_id=..., k=5)` runs a similarity search on the deployment's memory feature group, `agent_memory_<deployment id>`, with the filters the agent's own `search` tool uses; the agent need not be running. The query must be embedded by the model that embedded the messages: it is read off the feature group's description and loaded with sentence-transformers by default (install it), or pass `embedder=` for another model, which is refused when its `model_id` differs. Without semantic search the agent has no such feature group and the result is `[]`.

Those read the groups below for you. Reading them by hand:

```python
fs = project.get_feature_store()
facts = fs.get_feature_group("agent_memory_facts", 1).read(online=True)
facts[facts.deployment_id.astype(str) == str(agent.id)][["owner", "scope", "state_key", "value"]]
```

`deployment_id` is **text** in the memory groups (`agent_memory_facts`, `_messages`, `_conversations`) and a number in the others: the memory store runs outside Hopsworks too, where a deployment id is any name, so compare it as a string — `== agent.id` silently matches nothing.

Online, not offline: the offline Delta table has no commits until a materialisation runs, so `hops sql` / Trino answers `DELTA_LAKE_INVALID_SCHEMA ... has no commits`. `owner` is the subject for `user` scope — an email where the client asserted one, a conversation id where it did not, which is the quickest way to see whether the paragraph above is biting. The other columns are `id, source_ref, written_by, updated_at, expires_at` (`fg.columns`; `fg.features` is deprecated). An evaluation trial's `user`-scope facts have the **trial's** conversation id as `owner` (`<run>/<task>/…`), never the person's — that is how to confirm a replay did not write into a real user's memory.

Streaming, multimodal content, memory tiers, progress events and the structure graph are in the protocol README: `python/hopsworks_agents/protocol/README.md`.
