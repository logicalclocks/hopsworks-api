---
description: FTI-pattern ML project assistant — checks Hopsworks cluster state and proposes the next concrete step (feature / training / inference pipeline). Optionally pass `f`, `t`, or `i` to focus on one pipeline.
argument-hint: "[f|t|i]"
---

Dispatch to the `hops-fti` sub-agent to drive the user's ML project forward.

Pass any argument through so the agent can focus on a single pipeline:
- `f` — feature pipeline
- `t` — training pipeline
- `i` — inference pipeline (batch or online)
- empty — review all three

Invoke the `hops-fti` agent with `$ARGUMENTS` (or "review all three pipelines" when empty).
The agent will:
1. Probe the live cluster via `hops context --json` plus targeted read commands.
2. Map state onto the FTI checklist (F0–F4 / T0–T4 / I0–I3).
3. Propose exactly one next concrete step, waiting for the user before acting.
