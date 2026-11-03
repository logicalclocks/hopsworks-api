# Writing your own checks

Reference for **hops-agent-evals**. A `python` check is a function the project wrote, graded like a built-in one. Use it when a correctness rule is none of the built-in kinds and an LLM judge would be slower, costlier or less deterministic than code: the quoted price equals the catalogue price, the answer validates against your JSON schema, the refund amount in the tool call equals the one in the answer.

## Declare it

```python
# evaluation/checks/pricing.py
import json
from hopsworks_agents.eval.checks import check, CheckContext, CheckResult, ungradable

@check(needs_trace=True, description="Quoted price equals the catalogue price")
def price_matches_catalogue(ctx: CheckContext, tolerance: float = 0.01):
    quoted = _price_in(ctx.answer)
    lookups = ctx.calls("lookup_product")
    if not lookups:
        return ungradable("the agent never looked the product up")
    listed = float(lookups[-1].result_json["price"])
    ok = abs(quoted - listed) <= tolerance
    return CheckResult(passed=ok, score=1.0 if ok else 0.0,
                       reason="" if ok else f"quoted {quoted}, catalogue says {listed}",
                       assertions={"quoted": quoted, "listed": listed})

@check("answers_in_english", needs_trace=False)
def _english(ctx):
    return "the" in ctx.answer.lower()
```

- `@check` bare, `@check("name")`, or `@check(needs_trace=..., reads_expectation=..., timeout_s=..., description=...)`. The name defaults to the function's; the docstring is the default description.
- The first argument is the `CheckContext`; the keyword parameters after it are the check's settings, and their defaults are what a suite gets when it sets no `params`.
- Return `True`/`False`, a score from 0 to 1 (passes at `pass_score`, default 1.0), a `CheckResult(passed, score, reason, assertions)`, or `ungradable(reason)` when the precondition did not hold. A raised exception, a timeout or a return value that is none of these makes the trial **ungradable with the reason**, never a pass and never the agent's fault.
- `needs_trace=True` (the default) reports the check ungradable when no trace arrived, as every trajectory check does. `reads_expectation=True` keeps it out of monitors, which grade live traffic that carries no task.
- A check that needs the platform calls `hopsworks.login()` itself; inside the job that logs in as the project with no arguments.

## What the context holds

| Field | What it is |
|---|---|
| `ctx.question` | what the user asked; every turn numbered for a conversation task |
| `ctx.messages` | the task's input messages, as role/content dicts |
| `ctx.transcript` | the whole exchange as text, for a multi-turn trial |
| `ctx.answer`, `ctx.answer_json()` | the agent's final reply, and it parsed as JSON |
| `ctx.expectation`, `ctx.expectations` | what the task expects of this check (by its name), and of every check |
| `ctx.tools`, `ctx.tool_names`, `ctx.calls(name)` | the trace's tool calls in order: `name`, `arguments`/`arguments_json`, `result`/`result_json`, `status`, `failed`, `duration_ms` |
| `ctx.trace` | the raw trace dict, or `None` |
| `ctx.trial`, `ctx.task` | ids, status, latency, tokens; task type, category, tags, subject, context |
| `ctx.params` | the parameters it was configured with, merged over the defaults |

## Try it before a run does

```python
from hopsworks_agents.eval.checks import run_check

run_check(price_matches_catalogue, answer="It costs 19.99",
          tools=[{"name": "lookup_product", "result": '{"price": 19.99}'}], tolerance=0.01)

trace = agent.trace("4bf1...")                         # a real conversation
run_check(price_matches_catalogue, trace=trace)

for trial, verdict in agents.runs.replay(run, price_matches_catalogue):   # last night's run, re-graded
    print(trial.task_id, verdict.passed, verdict.reason)
```

`run_check` returns the exact row the job would record. `replay` writes nothing.

## Put it where the job reads it

One of three sources per check row; the code is never in the spec.

```python
# 1. A file in the project (the picker opens on Resources/agent_evals/checks)
agents.checks.upload("evaluation/checks/pricing.py", tags=[agent.name])
#    -> uploads, and saves one library entry per declared check

# 2. A git repository, or the deployment's own
agents.checks.register("evaluation/checks/pricing.py",
                       git={"url": "https://github.com/acme/agent.git", "ref": "main"})
agents.checks.register("evaluation/checks/pricing.py", git="deployment", tags=[agent.name])

# 3. Written by hand into a suite
check("python", "price_matches_catalogue", file="Resources/agent_evals/checks/pricing.py",
      params={"tolerance": 0.005})
check("python", "price_matches_catalogue", git={"url": "https://...", "ref": "main", "commit": "a1b2c3d"},
      path="evaluation/checks/pricing.py")
check("python", "price_matches_catalogue", git="deployment", path="evaluation/checks/pricing.py")
check("python", "short_answer", module="customer_checks")      # installed in the job's environment
```

| Key | Meaning |
|---|---|
| `file` | a `.py` file or a package directory (with `__init__.py`) in the project's HopsFS, relative to the project or absolute |
| `git` + `path` | `{"url": https, "ref": branch or tag, "commit": optional}`; `path` is the file inside the repository. Private repositories need a git provider token on the account the evaluation job runs as. https only. |
| `git: "deployment"` + `path` | the evaluated deployment's own repository at the commit it is running; checks version with the agent, and a deployment that is not git-backed makes the check ungradable |
| `module` | a module importable from the agent-pipeline environment the job runs in; install the package there |
| `name` / `check` | `name` is what results report under and, by default, the declared check it runs; `check` names another declared check to run under this name (two rows, two params) |
| `function` | a plain, undecorated function in the file |
| `params` | keyword arguments for the function, merged over its defaults |
| `needs_trace`, `reads_expectation`, `timeout_s` (max 600) | override what the decorator declares |

**Freezing.** A published suite is immutable. A `file` check is copied beside the suite's task snapshot when it is published; a `git` check is pinned by the job on the first run of the published version (the commit and hash go into `manifest.json` next to it) and every later run reads that copy. `git: "deployment"` is deliberately not frozen, and `module` cannot be: the environment is its version. Every result's `assertions.source` says what code graded it (`origin`, `sha256`, `commit`, `pinned`).

**Monitors.** A check that does not read `ctx.expectation` can grade live traffic like any other expectation-free check; the job reads the live file or module each run.
