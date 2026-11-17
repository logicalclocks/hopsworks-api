# Traces, feedback and failure analysis: more rules

The calls are in SKILL.md, section 3.

**`agent.regressions()` is not a `Suite`.** It returns a `RegressionSuite`: a status report on the standing suite — `exists`, `suite_id`, `name`, `version`, `published_version`, `task_count`, `published_task_count`, `unpublished_task_count`, `judge_check` — with no `.tasks()` and no `.checks`. It answers "is there anything waiting to be published", which is what you want before a release. For the suite itself, `agents.suites.get(reg.suite_id)`.

**`agent.traces()` lists conversations, not turns.** Each row is a session's latest turn, so a six-turn conversation is one row; `agent.session(trace.session_id)` opens the turns.

**A promotion carries the conversation.** The task takes every user turn up to the one the verdict is about, because a failure on the fourth turn cannot be reproduced by asking the fourth question of a fresh agent. `promote_feedback(row, conversation=False)` takes that turn alone when it stands on its own. `feedback.promoted_task_id` says whether a verdict already became a task.

**…and the person it happened to.** A promoted task is asked as whoever the original turn ran as — `task.subject`, sent as the request's `subject` on every turn — with the request `context` the client sent (`task.request_context`). Both are read off the trace's turn span, for every way of promoting: a trace, a verdict, a cluster. Without them a memory or personalisation failure cannot be a regression at all: "what do you already know about me?" answered "nothing" is a *correct* answer to a stranger, so asked anonymously the regression passes every time.

```python
task = agent.promote_feedback(row)                    # asked as the turn's subject
task = agent.promote_feedback(row, subject="")        # asked anonymously
task = agent.promote_feedback(row, subject="test-user@example.com")   # a test user with the same profile
task.confirm_redaction(subject="test-user@example.com")               # or change it in review
agents.tasks.create("What do you know about me?", subject="marcus@example.com", context={"plan": "pro"})
suite.add_task("What do you know about me?", subject="marcus@example.com")   # straight into a suite
```

**The regression suite is for promoted tasks.** `add_to_regressions` needs a task promoted from a deployment's trace — that is how it knows which deployment's standing suite to join — and refuses a hand-written one ("was not promoted from a deployment's trace"). Put a hand-written memory test in an ordinary suite with `suite.add_task(..., subject=...)`.

The subject is a customer's address copied out of a trace, so it is part of what the redaction review checks, and the Promotion queue shows it ("as marcus@example.com"). A multi-turn trial without a `subject=` keeps the person the app bound on its first turn for the turns after it, as a conversation does. That binding is held by the agent's process, so on a deployment scaled to several replicas a later turn can arrive unbound; `subject=` never does. **A trial reads that person's memory and never writes it**: under evaluation, `user`-scope writes go under the trial's own conversation and reads look there first, and the trial's messages are not filed in the person's conversation history — so a nightly regression run cannot rewrite a customer's stored profile. The same question asked as two people is two cases, for import dedup and for `changes()`.

**A replay reads that memory as it is now, not as it was.** A failure that happened *because* the person had not yet told the agent something — the first conversation, before a profile existed — replays differently once they have: the agent now knows the profile and takes another path, and the judge grades a different reply. Promote such a failure with `subject=""` or a fresh test subject, so the replay starts from the same empty memory. And a failure that happens only some of the time wants `run_regressions(..., n_trials=3)`: one trial passes it as often as not.

**Judges are told who a request was for.** A task's `subject`, and a sampled turn's (from its trace), reaches the judge as a `<user>` section: the agent need not ask who they are, and what it says about them from memory is not invented. Without it *Asked who it is talking to* failed every client-identified turn, and *Hallucination* called the person's own name invented.

**Reading a trace.** Everything that says what a span *did* is an attribute: `openinference.span.kind` (`AGENT`, `LLM`, `TOOL`, `RETRIEVER`), `llm.model_name`, `input.value`, `output.value`, the token counts. The span itself carries little more than its name and its timestamps. Each span and each event is handed back with its own attributes as a dict, and the flat per-trace lists are there too — `trace.span_attributes` / `trace.event_attributes`, with `trace.attributes_of(span_id)` giving the same dict:

```python
for span in sorted(trace.spans, key=lambda s: s.get("startTimeNs") or 0):
    attrs = span["attributes"]
    print(attrs.get("openinference.span.kind"), span["name"], attrs.get("llm.model_name", ""))
```

`trace.tool_calls` is the tool spans already unpacked into `{name, arguments, result, status, span_id}`, which is what most trace-reading actually wants.

**Write the correction as the reply, not as a rule.** The standing regression suite grades with `matches_correction`, which reads the `corrected_answer` as the expected answer verbatim. Its first criterion is whether the reply *does what the correction does*, weighted above the rest and with a floor of 4: when the correction asks for the email address first, a reply that asks scores 5 though it answers nothing yet, and a full answer that skips the question fails however good it is. The other two criteria, no contradiction and safety, defer to the correction, so asking what the correction asks is never a safety fault. "Strategies must carry the disclaimer" grades the agent on vagueness; the reply it should have given grades it on the thing you meant. Put the rule in `note` or `expected_tool_behavior`. The judge is shown the agent's tool calls beside the reply, and the verdict's `expected_tool_behavior` is part of the expected answer, so a failure about tools ("propose_strategies only" answered by three analysts) fails its regression even when the reply reads well.

The analysis files its own feedback rows (`detector:*`, `judge:*`) at run start, skips traces a person already judged, and never looks at evaluation traffic on its own. A person's verdict on an evaluation trial's trace is accepted, though: `agent.give_feedback(trial.trace_id, "negative", corrected_answer=...)`, then `promote_feedback(row, conversation=False)`, is how a suite failure becomes a standing regression. With `read_source_code=True` it clones the agent's source at the deployed commit (the deployment's git source, or the script on HopsFS; private repositories through the user's git provider) and cites file and line in its findings. `dismiss_cluster(cluster, reason)` closes one as `working_as_intended`, `duplicate_of`, `cannot_reproduce` or `out_of_scope`.

Metrics for dashboards: `agent.trace_metrics()`, `agent.llm_metrics()` (calls, tokens, cost), `agent.tool_metrics()` (calls, errors, latency per tool), each over `since`/`until`.
