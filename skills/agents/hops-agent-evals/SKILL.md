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
- **Schedule.** Run suites on demand, on every deploy (`run_on_update`), or from the per-agent evaluation job on a cron.

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
    gate_metric="pass_rate", gate_threshold=0.9,
)
suite.add_task("Refund order 42", expectations={"quality": "Confirms the refund and its amount.",
                                                "tool_call": "refund_order"})
suite.import_tasks([{"question": "Where is my order?"}, {"question": "Cancel it"}])
suite = suite.publish()                     # frozen; a run records the version it executed
suite.update(description="Refund flows")    # name, tags, description at any time; checks via set_checks()

run = agent.run(suite, n_trials=3).wait()   # n_trials > 1 is pass^k: every trial must pass
for trial in run.trials():
    print(trial.task_id, trial.status, trial.trace_id, trial.latency_ms)
for result in run.results():
    print(result.evaluator_name, result.passed, result.score, result.reason)
run.metrics()                               # pass rate per check and overall
agent.gates()                               # do the published suites' gates pass for this agent?
```

Check kinds, their configuration keys and how each reads a task's expectation are in [references/checks.md](references/checks.md). A check that needs an expected answer cannot grade live traces; the library tells you which.

**Draft vs published.** A draft is editable and cannot run; publishing freezes it. `suite.new_version()` copies a published suite into a new draft. Deleting a suite with runs needs `force=True`.

**Human review of a judge.** `run.review_trial(trial, passed=..., reason=..., evaluator="quality")` records a person's verdict per judge; the UI shows judge agreement per evaluator from these.

## 2. Online sampling and the evaluation job

```python
agents.evaluators.install_defaults()                  # Hallucination, Faithfulness, User frustration,
judge = agents.evaluators.find("Hallucination")       # Toxicity, Profanity, Bias and fairness,
agent.sample(evaluator=judge).wait()                  # Answer relevance, Safety
agent.sample(evaluator=judge, since=..., until=...)   # a window instead of "since last sample"

job = agents.jobs.ensure_eval_job(agent.id, suites=[suite], evaluators=[judge], monitor=True)
agents.jobs.run_eval_job(job.name)                    # one run per suite, plus the monitor sample
```

The evaluation job is a Hopsworks job named after the agent; schedule it like any job (**hops-job**). `monitor=True` samples traffic with the listed evaluators on every run. Runs list with `agents.runs.list(agent.id)`; `agents.runs.trend(agent.id)` gives the pass-rate trend. A running evaluation is stopped from the UI's Stop button (there is no client call yet).

## 3. Traces, feedback and failure analysis

```python
for trace in agent.traces(search="refund", search_field="messages", limit=20):
    print(trace.trace_id, trace.session_id, trace.subject, trace.latency_ms, trace.failed)
agent.trace(trace_id)                                 # spans, attributes, events, totals
agent.conversation(session_id)                        # user/assistant turns of one session

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
task = agent.promote_cluster(agent.clusters()[0])     # representative trace -> task, PENDING_REDACTION
task.confirm_redaction().add_to_regressions()         # a person checked it for personal data
agents.jobs.run_regressions(agent.id)                 # the standing <agent>_feedback_regressions suite
```

The analysis files its own feedback rows (`detector:*`, `judge:*`) at run start, skips traces a person already judged, and never looks at evaluation traffic. With `read_source_code=True` it clones the agent's source at the deployed commit (the deployment's git source, or the script on HopsFS; private repositories through the user's git provider) and cites file and line in its findings. `dismiss_cluster(cluster, reason)` closes one as `working_as_intended`, `duplicate_of`, `cannot_reproduce` or `out_of_scope`.

Metrics for dashboards: `agent.trace_metrics()`, `agent.llm_metrics()` (calls, tokens, cost), `agent.tool_metrics()` (calls, errors, latency per tool), each over `since`/`until`.

## Toolset
- **SDK:** `project.get_agent_serving()` → `AgentServing` (`suites`, `tasks`, `evaluators`, `runs`, `jobs`, `get_agent`) and `Agent` (`run`, `sample`, `traces`, `feedback`, `triage`, `clusters`, `analyse`, `gates`, metrics). Import `check` from `hopsworks_agents.eval.sdk`.
- **UI:** Deployments → agent → **Evals** tab (runs, feedback, failure analysis) and the **Evals** menu (suites, tasks, evaluator library, jobs).
- **CLI:** none yet; `hops job` operates the evaluation and analysis jobs once created.
- **REST:** `/project/{id}/agent-evals/*` and `/project/{id}/otel/servings/{deployment}/*`, only where the SDK does not cover it.

## Next steps
- Ship or change the agent: **hops-agent-deployment**.
- Schedule the evaluation or analysis job: [hops-job](../../platform/hops-job/SKILL.md).
- A judge that needs project data (RAG faithfulness against the feature store): **hops-fv**.
