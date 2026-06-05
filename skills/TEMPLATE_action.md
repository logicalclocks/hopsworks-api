---
name: <folder-name>            # MUST equal the directory name (skills route by folder)
description: <short. A routing trigger (when to auto-invoke) followed by the one-line input→output contract. Keep the auto-invoke keywords — the harness routes on this.>
---

# <Title>

One or two lines: what this workflow does and its role (e.g. the T in the FTI pattern).

## Contract
- **Input:** what the workflow consumes.
- **Output:** what it produces.
- **Pre-condition:** what must already exist / be true before starting.

## Smoke-test (cheap pre/post-flight)
```bash
hops <thing> list            # confirm state before; verify result after — no Python needed
hops <thing> info <name>
```

## Ask the user (only when state is ambiguous)
- The decisions that change the outcome (e.g. online vs offline, label column, split strategy).

## Steps (generic, non-binding)
1. ...

## Toolset
- **CLI:** `hops <thing> ...`
- **SDK:** `project.get_...()` / key calls
- **REST:** endpoints, only if the SDK doesn't cover it

## Next steps
- The skills to reach for once this is done (the FTI / workflow neighbours).
