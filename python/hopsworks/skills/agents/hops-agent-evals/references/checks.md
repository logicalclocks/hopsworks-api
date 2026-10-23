# Checks, judges and providers

Reference for **hops-agent-evals**. A suite's checks are `check(type, name=None, **config)` entries from `hopsworks_agents.eval.sdk`; a task supplies each check's expectation under the check's **name** in `expectations={...}`.

## Check kinds

| Kind | Config keys | Reads from the task | Grades |
|---|---|---|---|
| `exact_match` | `case_sensitive` (false) | expected text | final answer equals it |
| `contains` | `expected` (else per task) | expected text | final answer contains it |
| `regex` | `pattern`, `should_match` (true) | — | final answer matches (or must not) |
| `json_schema` | `required_keys` | — | final answer is JSON with the keys |
| `tool_call` | — | tool names, comma-separated; `!name` for one that must not be called (or `{"required": [...], "forbidden": [...]}`) | the trace called the required tools and none of the forbidden |
| `tool_order` | — | ordered tool names | tools called in that order |
| `no_tool_error` | — | — | no tool span errored |
| `tool_arguments` | `tool`, `required_keys`, `must_parse` (true) | — | the tool was called with those argument keys |
| `no_unnecessary_tools` | `allowed` | — | only allowed tools were called |
| `tool_retries` | `tool`, `max_retries` (0) | — | the tool was not retried beyond the budget |
| `tool_latency` | `tool`, `max_ms` | — | the tool's spans finished within budget |
| `sql_state` | `sql`/`query`, `expect` | — | a query against the agent's state returns the expectation (sandboxed suites) |
| `human_review` | `prompt` | — | waits for a person's verdict in the UI or `run.review_trial()` |
| `llm_judge` | provider keys below, `criteria` (a list, or a dict name → `{weight}`), `rubric`, `inputs` | the rubric (shown as the expected answer instead when `inputs` drops `rubric` and keeps `expected_result`) | a model grades the answer against the criteria |
| `pairwise` | provider keys, `reference` | reference answer | a model prefers the answer to the reference |
| `tool_arguments_judge` | provider keys, `tool` | — | a model judges the tool's arguments were right for the request |
| `tool_result_used` | provider keys | — | a model judges the answer used the tool's result |

Checks without a task expectation (`no_tool_error`, `tool_latency`, the judges whose `inputs` have neither `rubric` nor `expected_result`) can grade live traffic, which is what online sampling and `monitor=True` use. Checks that need an expectation only run in suites.

A judge saved without an explicit `inputs` takes the defaults — `user_request`, `expected_result`, `agent_response`, `rubric` — two of which only a task can supply, so it is suite-only by accident and a monitor refuses it. A judge meant for live traffic says what it reads: `inputs=["user_request", "agent_response", "tool_calls", "tool_results"]`.

## Criteria that may not apply

A criterion is scored on every trial, so one that describes a conditional behaviour is scored even when its condition never arose — and a judge with nothing to go on scores it low rather than skipping it. Three criteria, two of them inapplicable, is a failing trial for an agent that did the right thing.

Write each criterion so the inapplicable case passes:

```text
If the user gave a name instead of an email address, the reply asks for the address.
If an address was already given, score 5.
```

Or split the conditional case into its own task, where the condition is part of the question. Prefer the split when the two behaviours are really two requirements: one task per behaviour reads better in a report than one criterion that is sometimes vacuous.

## How a judge decides pass or fail

An `llm_judge` asks the model for one score per criterion on `score_range` (default `[1, 5]`), then decides the verdict itself:

1. **Weighted average.** `Σ(score × weight) / Σ(weight)`. Weights are relative — `{"a": 2, "b": 1}` and `{"a": 1, "b": 0.5}` are the same judge — default to 1, and may be 0 (scored and shown, never counted) but not all 0.
2. **Pass score.** The trial passes when the average is at least `thresholds={"pass_score": ...}`, in the same units as `score_range`. Default: three quarters of the way up the range — 4 on 1–5.
3. **Floors.** A criterion with a floor (`"critical_min"` on the criterion, or `thresholds={"critical_dimensions": {name: floor}}`; "never below" in the UI) fails the trial when it scores under it, **whatever the average**. Use one for anything that must never be traded away — safety, a required disclaimer — because a weight only dilutes a bad score.

```python
check("llm_judge", name="advice",
      criteria={
          "correct":            {"weight": 2, "description": "..."},
          "complete":           {"weight": 1, "description": "..."},
          "disclaimer_present": {"weight": 0.5, "critical_min": 4, "description": "..."},
      },
      thresholds={"pass_score": 3.5})
```

**The model never sees the weights or floors** — it is shown each criterion's name and description and scores them independently. Raising a weight does not make the judge stricter about that criterion; it only makes that criterion count for more in the average. To change how a criterion is *scored*, change its description.

A reply missing any criterion's score, or one that is not a number, makes the trial **ungradable**, not failed: an average over whichever criteria came back would be a different measurement each time. Out-of-range scores are clamped to `score_range` before anything else reads them.

The trial's `score` is the weighted average normalised to 0–1. Its `assertions` hold what the verdict was made from: `criteria` (raw scores), `criteria_normalised`, `weighted_score`, `pass_score`, `critical_breached` (the floors broken), `failure_category`, and the model's per-criterion `reasoning`. `reason` reads `scored 3.80 of 5, needs 4` or `below the floor on disclaimer_present`, followed by that reasoning — on passes as well as failures.

## Judge provider keys

Every judge check and the analysis job take the same keys:

- `provider`: `openai`, `anthropic`, `google`, `mistral`, `fireworks`, `groq`, `deepseek`, `xai`, `custom`.
- `model`: the provider's model id. `agents.evaluators.judge_models(provider)` lists what the provider offers with the project's key.
- `api_key_env`: the **name** of the environment variable holding the key; the key itself is never in the spec.
- `base_url`: the endpoint, for `custom` or to point a provider elsewhere (Azure, a gateway, a self-hosted vLLM).
- `headers`: extra headers as a dict; a value `$NAME` is replaced from the environment at call time (for gateway tokens).
- `reasoning_effort`: for models that take one.

Without `api_key_env` the provider's conventional variable is read (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, ...); set it for a job by adding it to your account's environment variables.

## Built-in judge templates

`agents.evaluators.install_defaults()` puts these in the project's library (rename, retune or delete as needed): **Faithfulness**, **Hallucination**, **User frustration**, **Toxicity**, **Profanity**, **Bias and fairness**, **Answer relevance**, **Safety**. Each is an `llm_judge` check with a rubric; save your own with `agents.evaluators.save(name, checks, description, tags=[...])`; tags are how an entry is found once the library is long (the agent it was written for, say), and `agents.evaluators.update(entry, tags=[...])` changes them.

## Failure-analysis job settings

`agents.jobs.ensure_review_job(agent.id, **settings)` / `job.update(**settings)`:

| Setting | Default | Meaning |
|---|---|---|
| `provider`, `model`, `api_key_env`, `base_url`, `headers`, `reasoning_effort` | anthropic | the triage model, keys as above |
| `sources` | `feedback,errors,judge` | signals to triage; add `anomalies` (latency and token outliers) |
| `read_source_code` | true | clone the agent's source at the deployed commit and cite file/line |
| `budget_calls` | 200 | model calls per run |
| `context_turns` | 20 | conversation turns shown to the model per trace |
| `auto_promote` + `auto_promote_min_cluster/confidence/acceptance/decisions` | off | promote a cluster to a task without a person, once calibration passes |
| `name`, `environment_name`, `cores`, `memory` | | the Hopsworks job itself |

Each triage carries `category`, `failure_summary`, `normalized_correction`, `suspected_code_bug` and `findings`; a finding has `file`, `line`, `finding`, `fix`, and when the model produced a patch, `original`, `replacement`, `verified` (the original text occurs exactly once in the file shown to it).
