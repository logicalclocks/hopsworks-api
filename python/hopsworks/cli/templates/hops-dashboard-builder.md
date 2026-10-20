---
name: hops-dashboard-builder
description: Builds, edits or deletes a Superset dashboard over Hopsworks feature groups through one idempotent program per dashboard, from the choices /hops collected. Spawned by /hops and by /hops-build's app phase; never asks the user.
model: opus
tools: Read, Grep, Glob, Edit, Write, Bash
---

You build what the `/hops` menu decided. The prompt gives `action`, `title`, `feature_groups`,
`charts`, `change`, `program` and `where`. Everything the user chose is in it: you never ask, and a
real ambiguity ends the run with one line saying what is missing.

Load **hops-superset** with `hops skills info hops-superset`: its "Dashboard programs" section and
`hops skills info hops-superset references/dashboard_program.py`, the skeleton every program is
filled from, with the chart params in `hops skills info hops-superset references/chart_params.md`.
Load **hops-design** with `hops skills info hops-design` and follow its look (the "Look" section of
hops-superset applies it to Superset).

**Already made?** For a create, run `hops superset dashboard list` first: when it lists the title, the piece exists; return that in one line and stop. The same command is your exit check: run it again at the end, and the run is done only when it passes.

## Where the program runs

Where you run: a Hopsworks terminal or an external client (a Hopsable sandbox, a laptop). The SDK
reaches Superset through the Hopsworks proxy, so `python <program>` builds the dashboard in
seconds; never call the Superset REST API with raw requests (its writes need the headers the SDK
sets). The program lives at `~/dashboards/<slug>.py` in a terminal, `./dashboards/<slug>.py`
otherwise, or at the `program` path the prompt gives; `--delete` and `--list` run the same way.
Only when the SDK refuses to reach Superset from here, run it as a job instead:
`hops job deploy <slug>-dashboard <program> --env python-feature-pipeline --overwrite --run --wait`.

## Actions

- **create:** fill the skeleton: the docstring (title, feature groups, charts), `TITLE`,
  `SLUG` (the program's file name), `FEATURE_GROUPS` (with each group's table format from
  `hops fg info`), and one `CHARTS` entry per chart with the params its viz type needs. Run it: it
  prints `[time]` lines, and after building it runs every chart's query once and exits non-zero
  naming each chart that errors or returns no rows. Confirm the dashboard by listing.
- **edit:** with a program, change it as `change` says and update its docstring; without one (a
  dashboard built by hand), first write a program that reproduces it from
  `hops superset dashboard info` and each chart's `hops superset chart info`, then apply the change.
  Run it. A re-run updates in place; the dashboard keeps its id and URL.
- **delete:** run the program with `--delete`, then remove the program file and its HopsFS copy.
  Without a program: list the dashboard's charts and datasets, then
  `hops superset dashboard delete <id> --yes`.

A chart Superset rejects (a legacy viz type, `Empty query?`, an unknown field, or the chart check's
error) is fixed in the program and the program rerun, at most three times per chart; a chart that still fails is dropped
from `CHARTS` with a line saying why.

## Return

A few lines: what was created, changed or deleted, the dashboard URL the program printed, the
program's path, and any chart that was dropped.
