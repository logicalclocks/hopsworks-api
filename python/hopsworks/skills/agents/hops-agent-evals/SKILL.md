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
for result in run.results():                 # result.trial_id, not trace_id -- see below
    print(result.evaluator_name, result.passed, result.score, result.reason)
run.metrics()                               # pass rate per check and overall
agent.gates()                               # do the published suites' gates pass for this agent?
```

**A result says which trial, not which conversation.** `EvaluatorResult` carries `trial_id`; the trace is on the trial. A `Trial` has no transcript of its own: the conversation a multi-turn trial had is `agent.conversation(trial.trial_id)` (its session id is its trial id). A single-turn trial has no session, so that is `[]`: open it by `trial.trace_id`. Printing results alone leaves you with a page of verdicts and no way to open the conversation behind one, so join them:

```python
traces = {t.trial_id: t.trace_id for t in run.trials()}
for result in run.results():
    print(result.evaluator_name, result.passed, traces.get(result.trial_id))
```

Check kinds, their configuration keys and how each reads a task's expectation are in [references/checks.md](references/checks.md). A check that needs an expected answer cannot grade live traces; the library tells you which.

**A multi-turn task's trajectory checks grade the last turn.** `tool_call`, `tool_order`, `tool_arguments` and the rest read the trace of the final turn, because that is the trace the trial records. A judge gets the whole transcript — as **context**: it grades the final reply (`<agent_answer>`), and a criterion written as "the reply …" is graded on that turn alone. A behaviour that belongs to an earlier turn — an introduction, a disclaimer shown once, an email asked for at the start — must say so: *"in this reply or an earlier one in the conversation"* is graded across every reply. Otherwise the second turn of `["dana@example.com", "What can you do?"]` fails for not repeating what the first turn already did. A pipeline that spans turns — analyse, then propose, then plan — cannot be checked with one `tool_order` over the lot: give each step its own task, or ask a judge.

**Sandboxed suites.** A suite with `execution_mode="sandboxed"` is accepted against a deployment whose manifest advertises per-request evaluation (`AgentApp(eval_per_request=True)`) or that runs with `EVAL_MODE=true`. Otherwise the runner refuses it. A red-team suite therefore needs no separate deployment if the agent already honours per-request eval.

**Several runs at once.** `agent.run(...)` runs are concurrent — the one-execution-at-a-time rule belongs to the evaluation *job*, not to runs started by hand. A run is recorded `PENDING` and takes up to a minute before its first trial, so a loop that starts five suites gets five runs that all begin shortly after.

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

**Draft vs published.** A draft is editable and cannot run; publishing freezes it. Deleting a suite with runs needs `force=True`.

**A version is a row.** `agents.suites.list()` lists every version of every suite, because a run records the version it executed; `list(latest=True)` is one row per suite, the newest. `suites.delete(s, force=True)` deletes one version — `delete(s, force=True, all_versions=True)` tears the whole suite down.

`suite.new_version()` copies a published suite's **checks** into a new draft — **not its tasks**. The draft comes back with `task_count` 0 and you re-add them, which is the point: a new version usually exists because the tasks are changing. Copy them across yourself if you only meant to change a check.

**Human review of a judge.** `run.review_trial(trial, passed=..., reason=..., evaluator="quality")` records a person's verdict per judge; the UI shows judge agreement per evaluator from these.

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
)])
```

Every offending evaluator is named at once when the job is configured, so you do not find them one failed call at a time.

**A generic judge does not know your agent.** The installed defaults see the request, the reply and the tools — not the system prompt. An agent designed to ask for an email before answering gets failed by *Answer relevance* for doing exactly that, and one that identifies a user across conversations gets failed by *Hallucination* for a claim its own design makes true. Prefer a judge written for the agent, and before trusting a monitor, read a sample of its failures and record what you find with `run.review_trial(...)` — judge agreement per evaluator is built from those.

**A memory-backed answer looks invented to a judge.** What the agent recalled reaches the model in its system prompt; the judge sees the request, the reply and the tool results, so "you told me you are aggressive and short-term" has no visible source and *Hallucination* fails it. Any agent with long-term memory will be marked down for using it. Either say so in the judge's criteria (*facts about the user may come from stored memory and are not inventions*), or have the agent recall through a **tool** rather than through the prompt, so the recalled value is in the trace where a judge can see it.

**One form for both jobs, one model for both.** `judge_model` on the evaluation job is what any check that names no model of its own judges with, *and* what the failure analysis uses — a judge used to be configured inside every check and again on the analysis job. `ensure_eval_job(...)` creates the analysis job with the evaluation's own environment, resources and model, and the evaluation triggers it. **It is on by default** — a failure nobody explains is one somebody reads twice — so pass `analyse_failures=False` for a deployment that should not spend model calls on it, and `judge_model=` to name the model. The provider is guessed from the agent's framework when you name none: the OpenAI Agents SDK implies OpenAI, the Claude SDK implies Anthropic, everything else keeps the default — no schedule of its own, and only when a run produced a **new failure or a regression**, since the same failures again need no second explanation. Its advanced settings (sources, source code, auto-promotion thresholds) stay on the analysis job itself.

**The evaluation starts analyses on its own,** whenever a run produces a new failure or a regression, so the clusters can change between two of your scripts' calls. A "since the last run" analysis never starts twice at once — asking while one is going returns that run — but read `agent.clusters()` once and work from that list, or check `agents.runs.list(agent.id)` for a `FEEDBACK_REVIEW` still `RUNNING` first. An open cluster whose verdicts a later analysis re-filed elsewhere is left off the list.

**Check the pair matches before you rely on automatic analysis.** The two jobs hold the judge separately, and they can drift — a deployment recreated under a new id got the framework's default provider on its analysis job while keeping the evaluation's model, leaving `provider=anthropic, model=gpt-5.6-terra`, an impossible pair. Every analysis the evaluation triggered then failed with *"no API key: set the environment variable ANTHROPIC_API_KEY"*, and nothing said so except the run's own status. One line settles it:

```python
job, review = agent.eval_job(), agent.review_job()
assert (review.provider, review.model) == (job.judge_provider, job.judge_model)
```

If they disagree, re-saving the evaluation's judge re-syncs the analysis job: `agents.jobs.update_eval_job(job.name, judge_provider=..., judge_model=...)`.

**An agent gets its evaluation job with it**, empty — no suites, no monitors, nothing scheduled — so configuring evaluation starts from a form with the deployment's defaults rather than from a page asking you to create something first.

**The jobs go with the deployment.** Deleting an agent deletes its evaluation and analysis jobs. A job holding the default name that a deployment deleted earlier left behind is taken over by the new agent of that name, starting from what you pass rather than from the dead agent's suites and evaluators; one a live deployment still owns is refused, so pass `name=`.

**One job per deployment, one execution at a time.** `ensure_eval_job` returns the deployment's job whatever name is asked for — what used to be a second job is this one's suites, its monitors and its schedule — and configures it with whatever you pass, leaving unsaid settings alone. `ensure_eval_job(agent.id)` on its own therefore changes nothing, and `update_eval_job(job.name, ...)` is the same partial update by name. An empty list clears a list; only an omitted argument is left as it was. A run asked for while one is going is queued — recorded, `PENDING`, with no execution — and starts when the current one finishes, with everything else waiting folded into the same execution. `update_eval_job(name, parallel_runs=2)` lifts that for a deployment that can take two evaluations at once.

The evaluation job is a Hopsworks job named after the agent; schedule it like any job (**hops-job**). `monitor=True` samples traffic with the listed evaluators on every run. A **scheduled** fire grades live traffic only — the suites are the same frozen tasks every fire, at a model call per task — unless `suites_on_schedule=True`. Running the job yourself always runs everything it names.

Each sample grades at most `sample_ceiling` conversations of its window (default 500, max 10000), because every one of them costs a judge call per evaluator. When the window holds more: `sample_strategy="random"` (the default) grades a spread of the whole window and moves on, keeping the cost fixed — the run records `window_conversations` and `sampled_fraction`, so its pass rate reads as a sample rather than a census. `sample_strategy="oldest"` grades the oldest of them and closes the window there, so the next run continues from where this one stopped and nothing is skipped — exhaustive, but on an agent whose traffic outpaces the ceiling it measures an older and older slice while the backlog grows.

Every evaluator of one execution grades the **same** conversations: the random draw is seeded from the window they share, which is what makes "the hallucination judge passed this one and the helpfulness judge failed it" a question you can ask (and makes a re-run of a window grade what it graded before). `shared_sample=False` gives each its own draw.

**A sample grades one turn of a conversation.** A sampled conversation is one trial, graded on its **last** exchange: `user_request` and `agent_response` are the final turn's. An earlier turn that went wrong is not graded, so a monitor over multi-turn traffic measures how conversations end. Suites are where a specific turn is pinned down.

**So only a turn-invariant criterion belongs in a monitor.** "Introduced itself and showed the disclaimer" is about the *first* turn; pointed at a six-turn conversation it grades the last one and scores 1/6, and the agent looks broken when the evaluator was. The test: could this be true of any single exchange, read on its own? *Answer relevance*, *was the tool's result used*, *did the reply carry the disclaimer it needed* — yes. *Introduced itself*, *asked who it is talking to*, *ran the steps in order* — no; those are suite tasks, where the turn is pinned down.

Write the criterion so that "this did not come up" is a pass rather than a miss, too. A judge told "advice must carry the disclaimer" fails every turn that gave no advice: say *when the reply gives advice, it carries the disclaimer; a reply that gives none passes*. Otherwise a correct agent reads as failing most of its traffic, and the failure clusters fill with an evaluator's own wording.

**"Since the last sample" is the job's watermark, not yours.** `agent.sample(evaluator=...)` with no window continues from where the last sample of that evaluator stopped. Pass `since=`/`until=` — which is what a hand-run script usually does — and you grade that window whatever the job has already graded, so a `--since 24h` sample run right after the job re-grades the same conversations. The run says so: `window_conversations` and `sampled_fraction` are on its metrics.

**The judge's key.** A sample run reads the key from the account's environment variables: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` by provider, or `EVAL_JUDGE_API_KEY` for whichever provider the judge names. Set one of those on the *job's* environment, not only in your shell — a judge with no key fails every trial as ungradable, and ungradable is not a failing score (see **A trial nobody could grade**). A key whose account runs out of credits does the same from the call it ran out on: `429 insufficient_quota` on the trial's `reason`, the run's `ungradable_rate` rising. Top up, then re-run the window with `agents.jobs.run_eval_job(job.name, since=..., until=...)`; the draw is seeded by the window, so the same conversations are graded.

**A sample can fail for the platform's reasons, not the agent's.** Twelve monitors of one execution start together, and under that load a run can come back `FAILED` with `trials=0` and the HTTP error on `run.error_message` — a 500 listing the window or reading the trials feature group. The jobs retry such a failure twice before giving up, and the SDK retries a read (`run.metrics()` included) on a 5xx, so this is rare; when it happens, re-run the execution over the same window — `agents.jobs.run_eval_job(job.name, since=..., until=...)` — and, the draw being seeded by the window, every evaluator grades the same conversations again.

**Reading the run list.** `agents.runs.list(agent.id)` returns suite runs and online samples together — filter on `run.run_type` (`"ONLINE_SAMPLE"`, `"SUITE"`, `"FEEDBACK_REVIEW"`), and `run.sample_source` names the evaluator a sample graded with. `run_eval_job` returns a list of runs, one per monitor, all sharing one `execution_id` — once the job has picked them up; at creation it is `None`. One execution at a time: a run started while the job is busy sits `PENDING` until the execution ahead of it finishes, which for twelve monitors over a few conversations is about ten minutes, so give a poll that long. A sample run's `estimated_cost` is the cost of the **traffic** it graded (the conversations' own tokens), not of the judge calls, which no run reports.

**The analysis job.** `analyse_failures` creates a job named `<agent>_feedback_review`, which `agent.review_job()` reads back and configures: its provider and model, whether it reads the agent's source, which sources it triages (feedback, errors, judge) and its call budget.

`run_eval_job` takes `since`, `until`, `sample` and `strategy` for one execution's monitors, which is how you try an evaluator out on a window you choose without changing what every scheduled fire does (the UI's Run button asks for the same four). Runs list with `agents.runs.list(agent.id)`; `agents.runs.trend(agent.id)` gives the pass-rate trend.

**Which failures are new.** `run.changes()` — or `agents.runs.failure_history(agent.id)` for every suite's latest run — classifies each task against the run before it, the way a build view does. Both return a `list[SuiteFailureHistory]`, one per suite, with **counts** `new_failures`, `regressions`, `fixed`, `persistent`, `unanswered`, `passing` and a `tasks` list of `TaskFailure`:

```python
for suite in run.changes():
    print(suite.suite_name, suite.regressions, "regressed,", suite.fixed, "fixed")
    for task in suite.tasks:
        print(" ", task.task_id, task.status)
```

Read from the trials as stored, so it needs no job to have processed anything. This is the **Changes** dialog on a run in the Jobs tab.

For a task-by-task diff of two particular runs, `previous.compare_to(latest)` (or `agents.runs.compare(previous, latest)`) returns a comparison with `.same_suite` and `.unchanged` / changed lists of `TaskDiff(before=..., after=..., change=..., question=...)`.

**Comparing two runs.** `agents.runs.compare(previous, latest)` (or `previous.compare_to(latest)`) diffs them task by task and returns `.regressed`, `.fixed`, `.added`, `.removed`, `.ungraded` and `.unchanged` — the list a release gate reads. A task counts as passed only when *every* gradable trial of it passed (`pass^k`, not `pass@k`), the ungraded statuses (`INFRA_ERROR`, `EVALUATOR_ERROR`, `TRACE_MISSING`, `AWAITING_REVIEW`) are left out of the verdict, and a task present in one run only is reported as added or removed rather than as unchanged. `.same_suite` is False when the two runs ran different suites (then everything reads as added or removed) and `.partial` is True when one of them failed and may have graded only part of its suite. A running evaluation is stopped from the UI's Stop button (there is no client call yet).

**A trial nobody could grade.** Four statuses say the harness, not the agent, has no verdict: `INFRA_ERROR` (the request never reached the agent, or a 4xx from the harness), `EVALUATOR_ERROR` (the trace was there and no evaluator reached a verdict, such as a judge whose provider refused the call), `TRACE_MISSING` (no trace to grade) and `AWAITING_REVIEW` (a person has not answered yet). They are left out of every pass rate. A run that graded nothing records no `pass_rate` at all, and a gate reads that as missing evidence; `ungradable_rate` at run scope says how much of a run is in these statuses.

**An agent that did not answer is not an agent that answered wrongly.** `AGENT_ERROR` is the agent itself failing, such as a 500, a crash, or a model refusing every request (read the message in `trial.error_message`, then `hops agent logs`). It counts as a failure, since nobody got an answer, so the gates fail. But `agent_error_rate` at run scope says it is an outage: a `pass_rate` of 0.0 with `agent_error_rate` 1.0 and zero latency is an agent that was down. `changes()` reports such tasks as `unanswered`, not as new failures, and never uses a run the agent answered nothing in as the baseline, so the run after an outage is not credited with fixing everything.

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
```

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

**Write the correction as the reply, not as a rule.** The standing regression suite grades with `matches_correction`, which reads the `corrected_answer` as the expected answer verbatim. Its first criterion is whether the reply *does what the correction does*, weighted above the rest and with a floor of 4: when the correction asks for the email address first, a reply that asks scores 5 though it answers nothing yet, and a full answer that skips the question fails however good it is. The other two criteria, no contradiction and safety, defer to the correction, so asking what the correction asks is never a safety fault. "Strategies must carry the disclaimer" grades the agent on vagueness; the reply it should have given grades it on the thing you meant. Put the rule in `note` or `expected_tool_behavior`.

The analysis files its own feedback rows (`detector:*`, `judge:*`) at run start, skips traces a person already judged, and never looks at evaluation traffic on its own. A person's verdict on an evaluation trial's trace is accepted, though: `agent.give_feedback(trial.trace_id, "negative", corrected_answer=...)`, then `promote_feedback(row, conversation=False)`, is how a suite failure becomes a standing regression. With `read_source_code=True` it clones the agent's source at the deployed commit (the deployment's git source, or the script on HopsFS; private repositories through the user's git provider) and cites file and line in its findings. `dismiss_cluster(cluster, reason)` closes one as `working_as_intended`, `duplicate_of`, `cannot_reproduce` or `out_of_scope`.

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
