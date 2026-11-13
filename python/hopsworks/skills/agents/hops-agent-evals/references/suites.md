# Suites: more rules

The suite calls are in SKILL.md, section 1.

**A multi-turn task's trajectory checks grade the last turn.** `tool_call`, `tool_order`, `tool_arguments` and the rest read the trace of the final turn, because that is the trace the trial records. A judge gets the whole transcript — as **context**: it grades the final reply (`<agent_answer>`), and a criterion written as "the reply …" is graded on that turn alone. A behaviour that belongs to an earlier turn — an introduction, a disclaimer shown once, an email asked for at the start — must say so: *"in this reply or an earlier one in the conversation"* is graded across every reply. Otherwise the second turn of `["dana@example.com", "What can you do?"]` fails for not repeating what the first turn already did. A pipeline that spans turns — analyse, then propose, then plan — cannot be checked with one `tool_order` over the lot: give each step its own task, or ask a judge.

**Sandboxed suites.** A suite with `execution_mode="sandboxed"` is accepted against a deployment whose manifest advertises per-request evaluation (`AgentApp(eval_per_request=True)`) or that runs with `EVAL_MODE=true`. Otherwise the runner refuses it. A red-team suite therefore needs no separate deployment if the agent already honours per-request eval.

**Several runs at once.** `agent.run(...)` runs are concurrent — the one-execution-at-a-time rule belongs to the evaluation *job*, not to runs started by hand. A run is recorded `PENDING` and takes up to a minute before its first trial, so a loop that starts five suites gets five runs that all begin shortly after.

**Draft vs published.** A draft is editable and cannot run; publishing freezes it. Deleting a suite with runs needs `force=True`.

**A version is a row.** `agents.suites.list()` lists every version of every suite, because a run records the version it executed; `list(latest=True)` is one row per suite, the newest. `suites.delete(s, force=True)` deletes one version — `delete(s, force=True, all_versions=True)` tears the whole suite down.

`suite.new_version()` copies a published suite's **checks** into a new draft — **not its tasks**. The draft comes back with `task_count` 0 and you re-add them, which is the point: a new version usually exists because the tasks are changing. Copy them across yourself if you only meant to change a check.

**Human review of a judge.** `run.review_trial(trial, passed=..., reason=..., evaluator="quality")` records a person's verdict per judge; the UI shows judge agreement per evaluator from these.
