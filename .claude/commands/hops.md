---
description: Hopsworks menu. Explore data, start an ML system, build or edit a Superset dashboard or a Python app with a JavaScript UI, show status.
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

## Dispatch on the first word

| First word | Do |
| --- | --- |
| none | Ask "What do you want to do?": **Explore data**, **Build or resume an ML system**, **A dashboard**, **An app**. Then continue with that choice below. |
| `explore` | Explore (below). |
| `dashboard` | Dashboard (below), with the rest as the instruction. |
| `app` | App (below), with the rest as the instruction. |
| `status` | Status (below). |
| `ml` | Run the interview: invoke the `hops-ml` command with the Skill tool, passing the rest as its arguments, and follow it. |
| `reqs`, `data`, `features`, `train`, `infer`, `verify`, `stop`, `f`, `t`, `i` | Reply with one line: building and running phases is `/hops-build` (for example `/hops-build train`, `/hops-build verify`), and stop. |

Choosing **Build or resume an ML system** from the menu runs the interview the same way. When the
system's `requirements.status` is already `met`, reply with one line that `/hops-build` continues it.

## Explore

Read-only. Run what the user's words need: `hops fg list`, `hops fv list`, `hops datasource
list`, `hops search <term>`, `hops fg info <name>`, `hops fg features <name>`,
`hops fg preview <name> --n 10`, `hops sql "<query>"` with a `LIMIT`. Summarise in a few lines and
offer the next step with `AskUserQuestion` (look deeper, a dashboard over it, an app over it, an ML
system over it).

## Status

With a system listed above, run `python <slug>/status.py` and print its output verbatim. Without
one, list apps (`hops app list`) and, in a terminal, dashboards (`hops superset dashboard list`);
from an external client list the programs under `./dashboards/` instead, since Superset is
reachable only inside the cluster.

## Dashboard

Superset is reachable only inside the cluster: in a terminal you may run `hops superset ...`; from
an external client do not, and list the programs under `./dashboards/` instead.

1. List the dashboards (`hops superset dashboard list` in a terminal; `ls dashboards/` otherwise)
   and ask: edit one (each by title, marked when a program exists under `dashboards/`), create one,
   or delete one. With none, go to create.
2. **Create.** `hops fg list` and ask which feature groups to chart (multi-select; a group that is
   online only cannot be charted, since Superset reads the offline store). Read their columns with
   `hops fg features <name>`, then ask which charts to keep from a few you propose by name: a
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
