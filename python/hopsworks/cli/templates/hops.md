---
description: Hopsworks menu. Explore data, make a table and look at it, start an ML system, build or edit a Superset dashboard or a Python app with a JavaScript UI, show status.
argument-hint: "[explore | ml | dashboard | app | status] [instruction]"
model: haiku
---

You are running `/hops` with arguments: `$ARGUMENTS`

This command runs on a fast model for menus and questions only. You never write code, a
dashboard program, an app or `system.yaml` here: the building goes to a sub-agent on a stronger
model, and an ML system is built by `/hops-build` after the `/hops-ml` interview.

Already known, no need to look again:
- ML systems in this directory: !`ls -d */system.yaml 2>/dev/null | head -5 || true`
- Where this runs: !`test -n "$HOPSFS_USER_HOME_DIR" && echo "Hopsworks terminal, home $HOPSFS_USER_HOME_DIR" || echo "external client, repository $(pwd)"`

## Rules

- Ask with `AskUserQuestion`, at once and before any lookup the question does not need: one call,
  up to four questions, two to four options each, the recommended one first and marked
  "(Recommended)". Never end a turn with a question in prose.
- Look things up with the `hops` CLI only when a question needs the answer, and in one Bash call
  when you need several listings.
- No secrets in arguments or output. Never delete anything the user has not confirmed by name.
- `where` in a builder's prompt is `Hopsworks terminal` only when `$HOPSFS_USER_HOME_DIR` is set
  (the line above says which; without it, run `test -n "$HOPSFS_USER_HOME_DIR"`). A laptop, a
  CI runner or a Hopsable sandbox is an `external client`.

## Dispatch on the first word

| First word | Do |
| --- | --- |
| none | Ask "What do you want to do?": **Explore data**, **Make a table and look at it**, **Build or resume an ML system**, **A dashboard or an app** (then ask which). Then continue with that choice below. |
| `explore` | Explore (below). |
| an instruction to make something outside an ML system (a table, features, a model or candidates, scoring, a dashboard, an app, an analytics layer), alone or several together | Pieces (below), whatever its first word. |
| `dashboard` | Dashboard (below), with the rest as the instruction. |
| `app` | App (below), with the rest as the instruction. |
| `status` | Status (below). |
| `ml` | Run the interview: invoke the `hops-ml` command with the Skill tool, passing the rest as its arguments, and follow it. |
| `reqs`, `data`, `features`, `train`, `infer`, `verify`, `stop`, `f`, `t`, `i`, with an ML system listed above | Reply with one line: building and running phases is `/hops-build` (for example `/hops-build train`, `/hops-build verify`), and stop. Without a system, `features`, `train` and `infer` are Pieces (below). |

Choosing **Build or resume an ML system** from the menu runs the interview the same way. When the
system's `requirements.status` is already `met`, reply with one line that `/hops-build` continues it.

## Explore

Read-only. Run what the user's words need: `hops fg list`, `hops fv list`, `hops datasource
list`, `hops search <term>`, `hops fg info <name>`, `hops fg feature list <name>`,
`hops fg preview <name> --limit 10`, `hops sql "<query>"` with a `LIMIT`. `hops sql` is Trino
SQL: `date_diff('day', a, b)`, `date_trunc('month', ts)`, `approx_percentile`, a table is
`<feature group>_<version>`. Summarise in a few lines and offer the next step with
`AskUserQuestion` (look deeper, a dashboard over it, an app over it, an ML system over it), the
one the user's words point to marked "(Recommended)".

## Pieces

A request outside an ML system names what to make. Each thing to make is a piece, and this table
is the whole dispatch:

| Piece | Needs | Makes | Exit check | Builder |
| --- | --- | --- | --- | --- |
| table | nothing, or a data source | feature group `<t>` with rows | `hops fg preview <t> --limit 1` prints a row | **hops-data-builder** |
| features | feature groups with rows, or a data source | feature group `<f>` with rows, job `<f>-features` | `hops fg preview <f> --limit 1` prints a row | **hops-features-builder** |
| model | feature view `<v>`, or feature groups with rows and the label | model `<m>` registered on `<v>` | `hops model info <m>` shows `Feature view` `<v> v<n>`; a model the user names is made, whatever its view | **hops-model-builder** |
| inference | model `<m>`, and the feature view the user names or the one its training dataset reads | feature group `<m>_predictions` with rows; job `<m>-score` only on a schedule | `hops fg list` shows a feature group whose name or description names `<m>`, and `hops fg preview <it> --limit 1` prints a row | **hops-infer-builder** |
| dashboard | feature groups with rows | Superset dashboard `<title>` | `hops superset dashboard list` lists `<title>` | **hops-dashboard-builder** (Dashboard below) |
| app | what it reads: feature groups, a model or a deployment | app `<a>` serving | `hops app info <a>` shows `State` `RUNNING` | **hops-app-builder** (App below) |
| analytics layer | bronze: a source; silver: feature groups with rows, used as bronze by name, never copied; gold: silver tables | the layer's tables with rows | `hops fg preview <table> --limit 1` for each table its `system.yaml` lists | `/hops-bronze`, `/hops-silver`, `/hops-gold` |

1. **The goal.** Write down the pieces the request names, each with its asset name. Add a piece
   one of them needs only when that need's exit check fails: "a model on the deals" with no
   feature view makes the view inside the model piece; "score the deals daily with the best
   model" when a model exists is the inference piece alone.
2. **Run what is missing, in needs order.** For each piece, run its exit check. Passing: the piece
   exists; say so in one line and go on. Failing: spawn its builder with the Agent tool and its
   prompt below, then run the exit check again. Failing again: stop and say which check failed
   and what the builder reported.
3. **A check is a command on the cluster**, never what a builder or an earlier step said.
4. **No `system.yaml`**, build lease or `set.py` for pieces. "Make it a system" runs `/hops-ml`,
   which starts from what the checks find.
5. **A question about the data** ("then show where revenue comes from by country"): once the table
   exists, answer from a few `hops sql "<query>"` calls (Trino SQL, as in Explore) over `<t>_1`,
   each with a `GROUP BY` and a `LIMIT`: a short table or a few lines per breakdown. Never a
   dashboard or an app unless the user asked for one by name.
6. **Then** offer the next step with `AskUserQuestion`, the one the request points to marked
   "(Recommended)": the next piece, a dashboard or an app over the result, or the whole ML system.

Ask only what the request leaves open, in one `AskUserQuestion` call. Every prompt ends with
`where: <Hopsworks terminal | external client>`.

| Builder | Prompt |
| --- | --- |
| hops-data-builder | `name`, `rows` (10000 unless the user said), `columns` (name and type, one per line), `story` (what the question should find), `source` (`synthetic` or a data source) |
| hops-features-builder | `name`, `sources` (feature groups or a data source), `features` (name, meaning, type), `grain` (the key and the event time), `schedule` (a Quartz cron, or none) |
| hops-model-builder | `name`, `feature_view` (name:version, or the feature groups and the label to make one), `label`, `task` (classification, regression), `candidates` (what the user named, or none), `metric` |
| hops-infer-builder | `name`, `model` (name:version, or the best by a metric), `feature_view` (name:version), `population` (the entities to score, such as open deals, and the feature group that lists them), `schedule` |

## Status

With a system listed above, run `python <slug>/status.py` and print its output verbatim. Without
one, list apps (`hops app list`) and dashboards (`hops superset dashboard list`, which works from
anywhere).

## Dashboard

`hops superset dashboard list` works from anywhere; every other `hops superset` command and the
SDK reach Superset only inside the cluster, so the builder runs its program as a job from an
external client.

1. List the dashboards (`hops superset dashboard list`) and ask: edit one (each by title, marked when a program exists under `dashboards/`), create one,
   or delete one. With none, go to create.
2. **Create.** `hops fg list` and ask which feature groups to chart (multi-select; a group that is
   online only cannot be charted, since Superset reads the offline store). Read their columns with
   `hops fg feature list <name>`, then ask which charts to keep from a few you propose by name: a
   big-number total, a bar over the categorical column with the fewest distinct values, a
   histogram of a numeric column, a time series on the event time when there is one. Ask the title,
   proposing one from the table names.
3. **Edit.** Ask which dashboard and what should change, in words.
4. **Delete.** Ask the user to confirm the exact title.
5. Spawn **hops-dashboard-builder** with the Agent tool and this prompt, then print what it returns:

   ```
   action: create | edit | delete
   title: <title>
   feature_groups: [<name>:<version>, ...]
   charts: <the charts the user kept, one per line, with the columns they use>
   change: <for an edit, the user's words>
   program: <dashboards/<slug>.py, or the ML system's <slug>/dashboards/<slug>.py>
   where: <Hopsworks terminal | external client>
   ```

## App

1. `hops app list`. An instruction naming an existing app is an edit; "delete" is a delete;
   otherwise a new app. With several candidates, ask which.
2. **Create.** The description is the instruction; without one, ask what the app should show, what
   a person can do in it, and what it reads (feature groups, feature views, a deployment, a model).
   Ask the name, proposing a slug from the description. When the working directory is a GitHub
   repository, ask whether the app is git-backed (a push redeploys it) or kept in HopsFS
   (recommended; no repository needed).
3. **Edit.** Ask what should change, in words, unless the instruction says it.
4. **Delete.** Ask the user to confirm the exact name.
5. Spawn **hops-app-builder** with the Agent tool and this prompt, then print what it returns:

   ```
   action: create | edit | delete
   name: <name>
   description: <the user's words>
   change: <for an edit, the user's words>
   source: <hopsfs | git>
   where: <Hopsworks terminal | external client>
   ```
