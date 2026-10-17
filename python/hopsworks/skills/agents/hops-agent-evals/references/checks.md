# Checks, judges and providers

Reference for **hops-agent-evals**. A suite's checks are `check(type, name=None, **config)` entries from `hopsworks_agents.eval.sdk`; a task supplies each check's expectation under the check's **name** in `expectations={...}`.

## Check kinds

| Kind | Config keys | Reads from the task | Grades |
|---|---|---|---|
| `exact_match` | `case_sensitive` (false) | expected text | final answer equals it |
| `contains` | `expected` (else per task) | expected text | final answer contains it |
| `regex` | `pattern`, `should_match` (true) | — | final answer matches (or must not) |
| `json_schema` | `required_keys` | — | final answer is JSON with the keys |
| `tool_call` | — | tool names, comma-separated; `!name` for one that must not be called | the trace called those tools |
| `tool_order` | — | ordered tool names | tools called in that order |
| `no_tool_error` | — | — | no tool span errored |
| `tool_arguments` | `tool`, `required_keys`, `must_parse` (true) | — | the tool was called with those argument keys |
| `no_unnecessary_tools` | `allowed` | — | only allowed tools were called |
| `tool_retries` | `tool`, `max_retries` (0) | — | the tool was not retried beyond the budget |
| `tool_latency` | `tool`, `max_ms` | — | the tool's spans finished within budget |
| `sql_state` | `sql`/`query`, `expect` | — | a query against the agent's state returns the expectation (sandboxed suites) |
| `human_review` | `prompt` | — | waits for a person's verdict in the UI or `run.review_trial()` |
| `llm_judge` | provider keys below, `criteria` (a list, or a dict name → `{weight}`), `rubric`, `inputs` | expected result (when `inputs` includes `expected_result`) | a model grades the answer against the criteria |
| `pairwise` | provider keys, `reference` | reference answer | a model prefers the answer to the reference |
| `tool_arguments_judge` | provider keys, `tool` | — | a model judges the tool's arguments were right for the request |
| `tool_result_used` | provider keys | — | a model judges the answer used the tool's result |

Checks without a task expectation (`no_tool_error`, `tool_latency`, the judges without `expected_result`) can grade live traffic, which is what online sampling and `monitor=True` use. Checks that need an expectation only run in suites.

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

`agents.evaluators.install_defaults()` puts these in the project's library (rename, retune or delete as needed): **Faithfulness**, **Hallucination**, **User frustration**, **Toxicity**, **Profanity**, **Bias and fairness**, **Answer relevance**, **Safety**. Each is an `llm_judge` check with a rubric; save your own with `agents.evaluators.save(name, checks, description)`.

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
