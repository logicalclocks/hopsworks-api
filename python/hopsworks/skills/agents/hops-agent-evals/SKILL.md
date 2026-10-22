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

suite = agents.suites.create(
    "Refunds",
    checks=[
        check("llm_judge", "quality", provider="anthropic", model="claude-sonnet-5",
              api_key_env="ANTHROPIC_API_KEY",
              criteria=["Answers the question", "Uses the customer key the user gave"]),
        check("tool_call"),                 # each task names the tool it expects
        check("no_tool_error"),
    ],
    tags=["regression"],
    execution_mode="read_only",             # or "sandboxed" for tasks that make the agent write
                                            # (see "Sandboxed suites" below for what accepts one)
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
for result in run.results():
    print(result.evaluator_name, result.passed, result.score, result.reason)
run.metrics()                               # pass rate per check and overall
agent.gates()                               # do the published suites' gates pass for this agent?
```

Check kinds, their configuration keys and how each reads a task's expectation are in [references/checks.md](references/checks.md). A check that needs an expected answer cannot grade live traces; the library tells you which.

**A multi-turn task's trajectory checks grade the last turn.** `tool_call`, `tool_order`, `tool_arguments` and the rest read the trace of the final turn, because that is the trace the trial records. A judge gets the whole transcript, so it can be asked about the conversation. A pipeline that spans turns — analyse, then propose, then plan — cannot be checked with one `tool_order` over the lot: give each step its own task, or ask a judge.

**Sandboxed suites.** A suite with `execution_mode="sandboxed"` is accepted against a deployment whose manifest advertises per-request evaluation (`AgentApp(eval_per_request=True)`) or that runs with `EVAL_MODE=true`. Otherwise the runner refuses it. A red-team suite therefore needs no separate deployment if the agent already honours per-request eval.

**Several runs at once.** `agent.run(...)` runs are concurrent — the one-execution-at-a-time rule belongs to the evaluation *job*, not to runs started by hand. A run is recorded `PENDING` and takes up to a minute before its first trial, so a loop that starts five suites gets five runs that all begin shortly after.

**Reading `run.metrics()`.** It returns a row per scope, not a summary: `run`, `evaluator`, `evaluator_family`, `task` and `score_bucket`, which on a six-task suite is about fifty rows. Start from the run scope:

```python
overall = {m.metric_name: m.metric_value for m in run.metrics() if m.metric_scope == "run"}
per_check = [m for m in run.metrics() if m.metric_scope == "evaluator"]
```

`estimated_cost` and `costed_trial_rate` are in there too. Cost is derived from the model named on a trial's spans against the platform's price table, so an agent whose framework does not record the model, or one on a model the table does not price, reports `0.0` with a `costed_trial_rate` of `0.0` — that rate is how you tell "free" from "not priced".

**Gate and canary.** A published suite created with `run_on_update=True` is a canary: updating the
deployment starts it on its own, and `agent.canary()` starts the same suites by hand. A run that
begins while the pod is still coming back waits for the agent to answer instead of failing, and
refuses the suite only after five minutes of silence. `agent.gates()` reports every suite that
declared a `gate_metric` and `gate_threshold`, each with `passed`, plus `has_evidence` for the whole
deployment: a gating suite that has never run against it counts as no evidence rather than as a
pass. Gates are advisory — they report, they do not block a deploy.

**Draft vs published.** A draft is editable and cannot run; publishing freezes it. `suite.new_version()` copies a published suite into a new draft. Deleting a suite with runs needs `force=True`.

**Human review of a judge.** `run.review_trial(trial, passed=..., reason=..., evaluator="quality")` records a person's verdict per judge; the UI shows judge agreement per evaluator from these.

## 2. Online sampling and the evaluation job

```python
agents.evaluators.judge_models("openai", secret="openai_api_key")   # pre-flight: does the key work,
                                                      # and is the model name real? [] means no key
                                                      # was found, not that there are no models
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
)])
```

Every offending evaluator is named at once when the job is configured, so you do not find them one failed call at a time.

**A generic judge does not know your agent.** The installed defaults see the request, the reply and the tools — not the system prompt. An agent designed to ask for an email before answering gets failed by *Answer relevance* for doing exactly that, and one that identifies a user across conversations gets failed by *Hallucination* for a claim its own design makes true. Prefer a judge written for the agent, and before trusting a monitor, read a sample of its failures and record what you find with `run.review_trial(...)` — judge agreement per evaluator is built from those.

**One form for both jobs, one model for both.** `judge_model` on the evaluation job is what any check that names no model of its own judges with, *and* what the failure analysis uses — a judge used to be configured inside every check and again on the analysis job. `ensure_eval_job(...)` creates the analysis job with the evaluation's own environment, resources and model, and the evaluation triggers it. **It is on by default** — a failure nobody explains is one somebody reads twice — so pass `analyse_failures=False` for a deployment that should not spend model calls on it, and `judge_model=` to name the model. The provider is guessed from the agent's framework when you name none: the OpenAI Agents SDK implies OpenAI, the Claude SDK implies Anthropic, everything else keeps the default — no schedule of its own, and only when a run produced a **new failure or a regression**, since the same failures again need no second explanation. Its advanced settings (sources, source code, auto-promotion thresholds) stay on the analysis job itself.

**An agent gets its evaluation job with it**, empty — no suites, no monitors, nothing scheduled — so configuring evaluation starts from a form with the deployment's defaults rather than from a page asking you to create something first.

**One job per deployment, one execution at a time.** `ensure_eval_job` returns the deployment's job whatever name is asked for — what used to be a second job is this one's suites, its monitors and its schedule — and configures it with whatever you pass, leaving unsaid settings alone. `ensure_eval_job(agent.id)` on its own therefore changes nothing, and `update_eval_job(job.name, ...)` is the same partial update by name. An empty list clears a list; only an omitted argument is left as it was. A run asked for while one is going is queued — recorded, `PENDING`, with no execution — and starts when the current one finishes, with everything else waiting folded into the same execution. `update_eval_job(name, parallel_runs=2)` lifts that for a deployment that can take two evaluations at once.

The evaluation job is a Hopsworks job named after the agent; schedule it like any job (**hops-job**). `monitor=True` samples traffic with the listed evaluators on every run. A **scheduled** fire grades live traffic only — the suites are the same frozen tasks every fire, at a model call per task — unless `suites_on_schedule=True`. Running the job yourself always runs everything it names.

Each sample grades at most `sample_ceiling` conversations of its window (default 500, max 10000), because every one of them costs a judge call per evaluator. When the window holds more: `sample_strategy="random"` (the default) grades a spread of the whole window and moves on, keeping the cost fixed — the run records `window_conversations` and `sampled_fraction`, so its pass rate reads as a sample rather than a census. `sample_strategy="oldest"` grades the oldest of them and closes the window there, so the next run continues from where this one stopped and nothing is skipped — exhaustive, but on an agent whose traffic outpaces the ceiling it measures an older and older slice while the backlog grows.

Every evaluator of one execution grades the **same** conversations: the random draw is seeded from the window they share, which is what makes "the hallucination judge passed this one and the helpfulness judge failed it" a question you can ask (and makes a re-run of a window grade what it graded before). `shared_sample=False` gives each its own draw.

**A sample grades one turn of a conversation.** A sampled conversation is one trial, graded on its **last** exchange: `user_request` and `agent_response` are the final turn's. An earlier turn that went wrong is not graded, so a monitor over multi-turn traffic measures how conversations end. Suites are where a specific turn is pinned down.

**Reading the run list.** `agents.runs.list(agent.id)` returns suite runs and online samples together — filter on `run.run_type` (`"ONLINE_SAMPLE"`, `"SUITE"`, `"FEEDBACK_REVIEW"`), and `run.sample_source` names the evaluator a sample graded with. `run_eval_job` returns a list of runs, one per monitor, all sharing one `execution_id`.

**The analysis job.** `analyse_failures` creates a job named `<agent>_feedback_review`, which `agent.review_job()` reads back and configures: its provider and model, whether it reads the agent's source, which sources it triages (feedback, errors, judge) and its call budget.

`run_eval_job` takes `since`, `until`, `sample` and `strategy` for one execution's monitors, which is how you try an evaluator out on a window you choose without changing what every scheduled fire does (the UI's Run button asks for the same four). Runs list with `agents.runs.list(agent.id)`; `agents.runs.trend(agent.id)` gives the pass-rate trend.

**Which failures are new.** `run.changes()` — or `agents.runs.failure_history(agent.id)` for every suite's latest run — classifies each task against the run before it, the way a build view does: `.new` (the run before did not have this task), `.regressed` (passed then, fails now), `.repaired`, and `.still_failing` with an `age` in runs. Read from the trials as stored, so it needs no job to have processed anything. This is the **Changes** dialog on a run in the Jobs tab.

**Comparing two runs.** `agents.runs.compare(previous, latest)` (or `previous.compare_to(latest)`) diffs them task by task and returns `.regressed`, `.fixed`, `.added`, `.removed`, `.ungraded` and `.unchanged` — the list a release gate reads. A task counts as passed only when *every* gradable trial of it passed (`pass^k`, not `pass@k`), harness errors (`INFRA_ERROR`, `EVALUATOR_ERROR`) are left out of the verdict, and a task present in one run only is reported as added or removed rather than as unchanged. `.same_suite` is False when the two runs ran different suites (then everything reads as added or removed) and `.partial` is True when one of them failed and may have graded only part of its suite. A running evaluation is stopped from the UI's Stop button (there is no client call yet).

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

for cluster in agent.clusters():                      # open clusters, most worth attention first
    print(cluster.label, cluster.size, cluster.severity)
task = agent.promote_cluster(agent.clusters()[0])     # -> task, PENDING_REDACTION
task.confirm_redaction().add_to_regressions()         # a person checked it for personal data
agent.promote_feedback(row)                           # one verdict, with its conversation
agents.jobs.run_regressions(agent.id)                 # the standing <agent>_feedback_regressions suite
```

**`agent.traces()` lists conversations, not turns.** Each row is a session's latest turn, so a six-turn conversation is one row; `agent.session(trace.session_id)` opens the turns.

**A promotion carries the conversation.** The task takes every user turn up to the one the verdict is about, because a failure on the fourth turn cannot be reproduced by asking the fourth question of a fresh agent. `promote_feedback(row, conversation=False)` takes that turn alone when it stands on its own. `feedback.promoted_task_id` says whether a verdict already became a task.

**Write the correction as the reply, not as a rule.** The standing regression suite grades with `matches_correction`, which reads the `corrected_answer` as the expected answer verbatim. "Strategies must carry the disclaimer" grades the agent on vagueness; the reply it should have given grades it on the thing you meant. Put the rule in `note` or `expected_tool_behavior`.

The analysis files its own feedback rows (`detector:*`, `judge:*`) at run start, skips traces a person already judged, and never looks at evaluation traffic. With `read_source_code=True` it clones the agent's source at the deployed commit (the deployment's git source, or the script on HopsFS; private repositories through the user's git provider) and cites file and line in its findings. `dismiss_cluster(cluster, reason)` closes one as `working_as_intended`, `duplicate_of`, `cannot_reproduce` or `out_of_scope`.

Metrics for dashboards: `agent.trace_metrics()`, `agent.llm_metrics()` (calls, tokens, cost), `agent.tool_metrics()` (calls, errors, latency per tool), each over `since`/`until`.

## Toolset
- **SDK:** `project.get_agent_serving()` → `AgentServing` (`suites`, `tasks`, `evaluators`, `runs`, `jobs`, `get_agent`) and `Agent` (`run`, `canary`, `sample`, `traces`, `feedback`, `triage`, `clusters`, `analyse`, `gates`, metrics). Import `check` from `hopsworks_agents.eval.sdk`.
- **UI:** Deployments → agent → **Evals** tab (runs, feedback, failure analysis) and the **Evals** menu (suites, tasks, evaluator library, jobs).
- **CLI:** none yet; `hops job` operates the evaluation and analysis jobs once created.
- **REST:** `/project/{id}/agent-evals/*` and `/project/{id}/otel/servings/{deployment}/*`, only where the SDK does not cover it.

## Next steps
- Ship or change the agent: **hops-agent-deployment**.
- Schedule the evaluation or analysis job: [hops-job](../../platform/hops-job/SKILL.md).
- A judge that needs project data (RAG faithfulness against the feature store): **hops-fv**.
