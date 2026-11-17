---
name: hops-agent-evals
description: Use when evaluating, monitoring or improving a deployed Hopsworks agent - writing evaluation suites and tasks, running them against an agent, grading live traffic with LLM judges, reading traces and feedback, running the failure analysis and turning its clusters into regression tests. Auto-invoke for "evaluate my agent", "agent evals", LLM-as-a-judge, "why is my agent failing", failure clusters, agent traces or feedback, `agents.suites`, `agent.run`, `agent.analyse`. Input a deployed agent; output suites, runs with pass/fail and metrics, and regression tasks from production failures.
---

# Hopsworks Agent Evaluation

The operate side of an agent: measure it before a release, watch it in production, and turn what breaks into tests. Everything the **Evals** pages of the UI do is on the project's agent-serving API, `project.get_agent_serving()`, next to the agents themselves (see **hops-agent-deployment**). Evaluation reads the agent's **traces**, so tracing must be enabled on the deployment; an untraced agent cannot be evaluated.

Three loops, cheapest first:
1. **Suites** — tasks with expectations, graded by checks, run on demand or on a schedule. Pass/fail per trial; a gate for promotion.
2. **Online sampling** — a saved judge grades a sample of real traffic since the last sample. No tasks needed.
3. **Failure analysis** — a model triages every negative signal (thumbs-down, tool errors, timeouts, judge failures), reads the agent's source, groups failures into clusters and proposes fixes; a person promotes a cluster into a regression task.

## Contract
- **Input:** a running, traced agent deployment; for suites, tasks with expectations; for judges, an LLM provider key stored as a project secret and named by `api_key_env`.
- **Output:** runs with trial results and metrics, feedback and triage on traces, failure clusters, and a standing `<agent>_feedback_regressions` suite.
- **Pre-condition:** the agent speaks the agent protocol (`AgentApp`) so trials can be correlated to traces; sandboxed suites need the agent to honour `in_evaluation()` in its writing tools (or a dedicated deployment with `EVAL_MODE=true`).

## Smoke-test (cheap pre/post-flight)

```python
agents = project.get_agent_serving()
agent = agents.get_agent("my_agent")
agent.tracing_ready()                 # the trace store answers
agent.traces(limit=3)                 # traces are arriving
agents.suites.list(); agents.runs.list(agent.id)
```

## Ask the user (only when state is ambiguous)
- **What "good" means.** Exact strings and tool calls (deterministic checks) or a rubric (LLM judge)? A judge needs a provider and a key; deterministic checks need expectations per task.
- **Read-only or sandboxed.** A suite whose tasks make the agent write (refunds, tickets) must be `sandboxed`; the agent must skip real writes under evaluation.
- **Provider for judges and analysis.** `anthropic`, `openai`, `google`, `mistral`, `fireworks`, `groq`, `deepseek`, `xai` or `custom` (any OpenAI-compatible endpoint with `base_url`, optional `headers`).
- **Schedule.** Run suites on demand, on every deployment update (`run_on_update`, the canary), or from the per-agent evaluation job on a cron.

## 1. Suites: author, publish, run

```python
from hopsworks_agents.eval.sdk import check

agent = agents.get_agent("refunds_agent")     # the deployment these are written for

suite = agents.suites.create(
    "Refunds",
    checks=[
        check("llm_judge", "quality", provider="anthropic", model="claude-sonnet-5",
              api_key_env="ANTHROPIC_API_KEY",
              criteria=["Answers the question", "Uses the customer key the user gave"]),
        check("tool_call"),                 # each task names the tool it expects
        check("no_tool_error"),
    ],
    tags=[agent.name, "regression"],         # the agent's name first: see "Tag with the agent" below
    execution_mode="read_only",             # or "sandboxed" for tasks that make the agent write
                                            # (references/suites.md, "Sandboxed suites", says what accepts one)
    gate_metric="pass_rate", gate_threshold=0.9,
)
suite.add_task("Refund order 42", expectations={"quality": "Confirms the refund and its amount.",
                                                "tool_call": "refund_order"})
suite.add_task(["Analyze GOOGL.", "I'm conservative. Strategies?"])   # a list is a conversation
suite.import_tasks([{"question": "Where is my order?"}, {"question": "Cancel it"}])

task = suite.tasks()[0]                     # while the suite is a draft:
task.edit("Refund order 43")                # what it asks; a list makes it multi-turn
task.edit(expectations={"quality": "Names the amount."})   # only what you pass changes
task.attach("chart.png")                    # a picture or a recording the question is about
task.attachment("chart.png")                # its bytes back; remove_attachment() drops it
suite = suite.publish()                     # frozen; a run records the version it executed
suite.update(description="Refund flows")    # name, tags, description at any time; checks via set_checks()

run = agent.run(suite, n_trials=3).wait()   # n_trials > 1 is pass^k: every trial must pass
                                            # several agent.run calls run concurrently, and a
                                            # run sits PENDING for up to a minute before trials
for trial in run.trials():
    print(trial.task_id, trial.status, trial.trace_id, trial.latency_ms)
for result in run.results():                 # result.trial_id, not trace_id -- see below
    print(result.evaluator_name, result.passed, result.score, result.reason)
run.metrics()                               # pass rate per check and overall
agent.gates()                               # do the published suites' gates pass for this agent?
```

**Tag with the agent.** Suites and evaluators belong to the project, not to an agent, so once a project has several agents the only way to find one agent's suites and evaluators is by tag. Give every suite and every evaluator you create for an agent its name as a tag, `tags=[agent.name, ...]`, beside what it covers (`regression`, `safety`, `red-team`): the Evals page filters both lists by tag, and `agents.evaluators.list()` entries carry them. For ones that exist already, `agents.suites.update(suite, tags=[...])` and `agents.evaluators.update(entry, tags=[...])` change the tags without a new version.

**A result says which trial, not which conversation.** `EvaluatorResult` carries `trial_id`; the trace is on the trial. A `Trial` has no transcript of its own: the conversation a multi-turn trial had is `agent.conversation(trial.trial_id)` (its session id is its trial id). A single-turn trial has no session, so that is `[]`: open it by `trial.trace_id`. Printing results alone leaves you with a page of verdicts and no way to open the conversation behind one, so join them:

```python
traces = {t.trial_id: t.trace_id for t in run.trials()}
for result in run.results():
    print(result.evaluator_name, result.passed, traces.get(result.trial_id))
```

Check kinds, their configuration keys and how each reads a task's expectation are in [references/checks.md](references/checks.md). A check that needs an expected answer cannot grade live traces; the library tells you which.

When no built-in kind says what correct means, write your own: functions decorated with `@check` in a file the project owns (uploaded with `agents.checks.upload(path)`, or in the agent's git repository). A suite names the file as one `python` row and every check it declares runs, each reported under its own name; try one locally with `run_check(...)` first. How to write, try and place one is in [references/custom-checks.md](references/custom-checks.md).

**Reading `run.metrics()`.** It returns a row per scope, not a summary: `run`, `evaluator`, `evaluator_family`, `task` and `score_bucket`, which on a six-task suite is about fifty rows. Start from the run scope:

```python
overall = {m.metric_name: m.metric_value for m in run.metrics() if m.metric_scope == "run"}
per_check = [m for m in run.metrics() if m.metric_scope == "evaluator"]
```

`estimated_cost` and `costed_trial_rate` are in there too. Cost is the trial's tokens against the **prices on the deployment**, which default to nothing — so an agent deployed without them reports `0.0` for every run, exactly as a free model would. `costed_trial_rate` of `0.0` is the giveaway. Set the prices when the agent is deployed, from the model it runs: see "Price the turns, or cost reads zero" in **hops-agent-deployment**.

**Gate and canary.** A published suite created with `run_on_update=True` is a canary: updating the
deployment starts it on its own, and `agent.canary()` starts the same suites by hand. A run that
begins while the pod is still coming back waits for the agent to answer instead of failing, and
refuses the suite only after five minutes of silence. `agent.gates()` reports every suite that
declared a `gate_metric` and `gate_threshold`, each with `passed`, plus `has_evidence` for the whole
deployment: a gating suite that has never run against it counts as no evidence rather than as a
pass. Gates are advisory — they report, they do not block a deploy.

More on suites (multi-turn trajectory checks, sandboxed suites, concurrent runs, drafts and versions, human review of a judge): `hops skills info hops-agent-evals references/suites.md`.

## 2. Online sampling and the evaluation job

```python
agents.evaluators.judge_models("openai", secret="openai_api_key")   # pre-flight: does the key work,
                                                      # and is the model name real? [] means no key
                                                      # was found, not that there are no models
agents.evaluators.judge_models("openai")              # no secret named: tries the secrets
                                                      # EVAL_JUDGE_API_KEY, OPENAI_API_KEY, openai_api_key
agents.evaluators.install_defaults()                  # Hallucination, Faithfulness, User frustration,
judge = agents.evaluators.find("Hallucination")       # Toxicity, Profanity, Bias and fairness,
agent.sample(evaluator=judge).wait()                  # Answer relevance, Safety
agent.sample(evaluator=judge, since=..., until=...)   # a window instead of "since last sample"
agent.sample(evaluator=judge, sample=50)              # cap the conversations graded; each is a judge call
agent.sample(suite=suite)                             # or grade live traffic with a suite's checks

job = agents.jobs.ensure_eval_job(agent.id, suites=[suite], evaluators=[judge], monitor=True)
agents.jobs.run_eval_job(job.name)                    # one run per suite, plus the monitor sample
agents.jobs.update_eval_job(job.name, suites_on_schedule=True)   # scheduled fires run the suites too
agents.jobs.update_eval_job(job.name, sample_ceiling=2000, sample_strategy="oldest")
agents.jobs.run_eval_job(job.name, since=..., until=..., sample=20)   # one run over a window
```

**A judge that will grade live traffic must say what it reads.** A judge's default inputs are `user_request`, `expected_result`, `agent_response` and `rubric` — two of which come from a task, which a live trace has none of. So a judge saved without an explicit `inputs` is suite-only by accident, and the monitor refuses it: *"is set to read the task's expected result, which a live trace has none of"*. Set them when you save it:

```python
agents.evaluators.save("answered the question", checks=[check(
    "llm_judge", "answered", inputs=["user_request", "agent_response", "tool_calls", "tool_results"],
    criteria={"answered": {"weight": 1, "description": "Does the reply answer what was asked?"}},
)], tags=[agent.name])
```

Every offending evaluator is named at once when the job is configured, so you do not find them one failed call at a time.

The judge's key, judges written for the agent, the analysis job beside the evaluation job, one job per deployment, sampling ceilings and strategies, turn-invariant monitor criteria, the run list, which failures are new, comparing runs, trial timeouts: `hops skills info hops-agent-evals references/monitoring.md`.

**A trial nobody could grade.** Four statuses say the harness, not the agent, has no verdict: `INFRA_ERROR` (the request never reached the agent, or a 4xx from the harness), `EVALUATOR_ERROR` (the trace was there and no evaluator reached a verdict, such as a judge whose provider refused the call), `TRACE_MISSING` (no trace to grade) and `AWAITING_REVIEW` (a person has not answered yet). They are left out of every pass rate. A run that graded nothing records no `pass_rate` at all, and a gate reads that as missing evidence; `ungradable_rate` at run scope says how much of a run is in these statuses.

**An agent that did not answer is not an agent that answered wrongly.** `AGENT_ERROR` is the agent itself failing, such as a 500, a crash, or a model refusing every request (read the message in `trial.error_message`, then `hops agent logs`). `TIMEOUT` is a turn that took longer than the trial may take. Both count as failures, since nobody got an answer, so the gates fail. But `agent_error_rate` and `timeout_rate` at run scope say which it was: a `pass_rate` of 0.0 with `agent_error_rate` 1.0 and zero latency is an agent that was down. `changes()` reports such tasks as `unanswered`, not as new failures, and never uses a run the agent answered nothing in as the baseline, so the run after an outage is not credited with fixing everything.

## 3. Traces, feedback and failure analysis

```python
for trace in agent.traces(search="refund", search_field="messages", limit=20):
    print(trace.trace_id, trace.session_id, trace.subject, trace.latency_ms, trace.failed)
agent.trace(trace_id)                                 # spans, attributes, events, totals
agent.session(session_id)                             # every turn of one conversation, oldest first
agent.conversation(session_id)                        # the same, paired: {trace_id, user, assistant}

agent.give_feedback(trace_id, "negative", issue_category="wrong_tool",
                    corrected_answer="Look the customer up by the key they gave.")
page = agent.feedback(verdict="negative"); page.count; page.feedback
agent.feedback_summary(since=..., until=...)          # verdict counts, by window and issue category

job = agents.jobs.ensure_review_job(agent.id, provider="anthropic", model="claude-sonnet-5",
                                    sources=["feedback", "errors", "judge"],   # + "anomalies"
                                    read_source_code=True)
run = agent.analyse().wait()                          # since the last run; or analyse(since=, until=) / trace_id=
for triage in agent.triage(page.feedback):
    print(triage.category, triage.failure_summary, triage.suspected_code_bug)
    for finding in triage.findings:                   # file, line, finding, fix, original/replacement
        print(finding["file"], finding["line"], finding["fix"])
    agent.decide_triage(triage, "accepted")           # accepted / edited / rejected: calibration data
    # agent.decide_triage(triage, "rejected")         # the analysis read the trace wrong: say so, so
    #                                                 # agent.calibration() counts it against it

for cluster in agent.clusters():                      # open clusters, most worth attention first
    print(cluster.label, cluster.size, cluster.severity)
task = agent.promote_cluster(agent.clusters()[0])     # -> task, PENDING_REDACTION
task.confirm_redaction().add_to_regressions()         # a person checked it for personal data
agent.promote_feedback(row)                           # one verdict, with its conversation
agents.jobs.run_regressions(agent.id)                 # publishes pending tasks, then runs them
agents.jobs.run_regressions(agent.id, n_trials=3)    # a failure that happens some of the time
# with nothing promoted yet it refuses: "this deployment has no regression suite yet;
# confirm a promoted task into it first"
```

Regression suites, promotion with the conversation and its subject, judges told who a request was for, reading a trace's spans, writing a correction, the analysis job's own rows, metrics for dashboards: `hops skills info hops-agent-evals references/traces.md`.

## Toolset
- **SDK:** `project.get_agent_serving()` → `AgentServing` (`suites`, `tasks`, `evaluators`, `runs`, `jobs`, `get_agent`) and `Agent` (`run`, `canary`, `sample`, `traces`, `feedback`, `triage`, `clusters`, `analyse`, `gates`, metrics). Import `check` from `hopsworks_agents.eval.sdk`.
- **UI:** Deployments → agent → **Evals** tab (runs, feedback, failure analysis) and the **Evals** menu (suites, tasks, evaluator library, jobs).
- **CLI:** none yet; `hops job` operates the evaluation and analysis jobs once created.
- **REST:** `/project/{id}/agent-evals/*` and `/project/{id}/otel/servings/{deployment}/*`, only where the SDK does not cover it.

## Next steps
- Ship or change the agent: **hops-agent-deployment**.
- Schedule the evaluation or analysis job: [hops-job](../../platform/hops-job/SKILL.md).
- A judge that needs project data (RAG faithfulness against the feature store): **hops-fv**.
