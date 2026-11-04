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
- Return `True`/`False`, a score from 0 to 1, a `CheckResult(passed, score, reason, assertions)`, or `ungradable(reason)` when the precondition did not hold. A bare score passes at `pass_score`, default 1.0; set it per check in the row's `params` (`{"scored": {"pass_score": 0.8}}`) -- it is read by the job, and handed to the function only when the function declares a `pass_score` parameter. `assertions` is written as JSON: a `Decimal`, a `datetime`, a set or a numpy scalar is stored as text or a list. A raised exception, a timeout or a return value that is none of these makes **that check's result ungradable with the reason**, never a pass and never the agent's fault; the trial keeps the verdicts of the other checks. A check that times out once is not called again in that run.
- `needs_trace=True` (the default) reports the check ungradable when no trace arrived, as every trajectory check does. `reads_expectation=True` keeps it out of monitors, which grade live traffic that carries no task.
- A check that needs the platform calls `hopsworks.login()` itself; inside the job that logs in as the project with no arguments.
- The file's directory is on the path while it is imported, so a helper beside it imports (`from helpers import ...`); the job reads project files off the HopsFS mount. Checks spread over several files can also be a package directory, whose `__init__.py` imports them; a git source is imported with its repository root on the path.
- Check code runs in the evaluation job, as the job's user, with the job's environment: whoever can write the file or push to the repository can run code there.

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

A suite's row names a source; every check the source declares runs, each reported under its own name, the way a test file is pointed at and every test in it runs. The code is never in the spec.

```python
# 1. A file in the project (the picker opens on Resources/agent_evals/checks)
agents.checks.upload("evaluation/checks/pricing.py", tags=[agent.name])
#    -> uploads, and saves one library entry for the file, describing what it declares

# 2. A git repository, or the deployment's own
agents.checks.register("evaluation/checks/pricing.py",
                       git={"url": "https://github.com/acme/agent.git", "ref": "main"})
agents.checks.register("evaluation/checks/pricing.py", git="deployment", tags=[agent.name])

# 3. Written by hand into a suite
check("python", "pricing", file="Resources/agent_evals/checks/pricing.py",
      params={"price_matches_catalogue": {"tolerance": 0.005}})
check("python", "pricing", git={"url": "https://...", "ref": "main", "commit": "a1b2c3d"},
      path="evaluation/checks/pricing.py", only=["price_matches_catalogue"])
check("python", "pricing", git="deployment", path="evaluation/checks/pricing.py")
check("python", "customer", module="customer_checks")      # installed in the job's environment
```

| Key | Meaning |
|---|---|
| `name` | the row's label; results report under each declared check's own name, with the row in `assertions.row`. When two rows of one suite declare the same check name, the row later in the suite reports it as `<row>/<check>` |
| `file` | a `.py` file or a package directory (with `__init__.py`) in the project's HopsFS, relative to the project or absolute under `/Projects/<this project>/`; a package is at most 2000 files / 20 MB |
| `git` + `path` | `{"url": https, "ref": branch or tag, "commit": optional}`; `path` is the file inside the repository. Private repositories need a git provider token on the account the evaluation job runs as; the url itself carries no credentials. https only. |
| `git: "deployment"` + `path` | the evaluated deployment's own repository at the commit it is running; checks version with the agent, and a deployment that is not git-backed makes the row ungradable |
| `module` | a module importable from the agent-pipeline environment the job runs in; install the package there |
| `only`, `exclude` | lists of declared check names, to run a subset; `only: []` runs nothing. For a file in the project the backend refuses a name in `only`, `exclude` or `params` the file does not declare when the row is saved; for a git or module source such a name is a skipped row that says so |
| `params` | keyed by check name, each value that check's keyword arguments, merged over its defaults |

Every library entry that holds a `python` row carries the tag `custom-code` (added by the backend whoever saves it), so `agents.evaluators.list()` filtered by that tag is the project's code checks. `needs_trace`, `reads_expectation`, `timeout_s` and the description are the decorator's and only the decorator's. For a file in the project the backend scans the file when the row is saved and keeps what it declares on the row, so the task form knows which checks read an expectation and a monitor refuses a file that declares one (narrow it with `only` or `exclude`). A git or module source runs whatever it declares.

**Freezing.** A published suite is immutable, and that covers a check's code where the code has a version to freeze. A `file` source is not copied: the job reads the project file off the HopsFS mount on every run, published or not, and each result records the hash of every file the import read (`source.files`, the entry and the helpers beside it, hashed as read), so the file stays the project's to edit and a run still says which text graded it. A `git` source is pinned by the job on the first run of the published version (the repository tree and a `manifest.json` with the commit and hash, in one directory) and every later run reads that copy; a repository over 2000 files / 20 MB is pinned by commit alone, and later runs clone at it. `git: "deployment"` is deliberately not pinned, and `module` cannot be: the environment is its version. Every result's `assertions.source` says what code graded it (`origin`, `sha256`, `commit`, `pinned`).

**Monitors.** A check that does not read `ctx.expectation` can grade live traffic like any other expectation-free check; the job reads the live file or module each run. A check declared `reads_expectation=True` is refused for a monitor when the file can be scanned, and is ungradable on every live trial otherwise.
