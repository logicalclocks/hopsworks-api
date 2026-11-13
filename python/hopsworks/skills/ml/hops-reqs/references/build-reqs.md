# /hops-build: the repository and reqs

Part of `/hops-build` (`hops-build.md`), read at the step that names it; the Rules and the lock there still hold.

## Before reqs: the repository

The interview created `<slug>/` from the system template and recorded `system.repo`: `url: new`
for a GitHub repository the build creates, `url: local` for code kept in Hopsworks only (a git
repository in `<slug>/` with no remote, committed on `main` at every phase and never pushed:
**Kept in Hopsworks only** in `hops-reqs/references/repo.md`), or the `url` and `provider` (`github`, `gitlab`,
`bitbucket` or `git`) of an existing repository the user gave. A `gitlab`, `bitbucket` or `git`
provider follows **Other git hosts** in `hops-reqs/references/repo.md`: git alone, no `gh`, a
merge request link in place of the pull request. For GitHub, follow `hops-reqs/references/repo.md`: GitHub access is any one
of the `gh` login, a GitHub token from Hopsworks Account Settings (as `GH_TOKEN`), or an SSH
key GitHub accepts. With only the SSH key there is no GitHub API: the user picks between adding
a login or token and pushing to an existing repository they name (push-only: no pull request, a
compare link instead); stop and say how to fix it only when none works. For `new`, create the
repository with `gh repo create` (owner from `gh api user`, name `hops-<slug>`, or
`hops-<slug>-<project>` when the owner already has a repository of that name, private) and record
its URL; cut `hops/<slug>` from the default branch, or `hops/<slug>-<project>` when `hops/<slug>`
holds another project's build, and record `system.repo`. An example in a repository of its own
(one the build created, or with no branch but the default and no commit but the init) works on
the default branch instead: each phase commits and pushes straight to it, with no branch and no
pull request, as repo.md says. A new repository has the system
directory as its root (`<slug>/` is the work tree, never the HopsFS home), as repo.md shows.
`<slug>/AGENTS.md`, from the template, tells an agent started there that the system is built from
`system.yaml` and how to follow a change downstream; it is committed with the rest of the
system.


## reqs: complete the specification

The `/hops-ml` interview has recorded, quickly and on a fast model: the problem in the user's
words, the system type, the cadence or the latency and throughput, the data sources (a new
connector already created, its table chosen), how predictions are consumed, the app wanted, and
where the code goes. Read them from `system.yaml`; never ask them again.
A system created from the Factory's form records only what the form asked, and nothing is asked in
the terminal: ask what it left unrecorded (no `requirements.data_sources`, no cadence or latency and
throughput, no `app`, no `system.repo`) in the first `AskUserQuestion` call, offering the project's
feature groups as the data options.
Complete the rest with the user through `AskUserQuestion`, two or three questions per call with a recommended answer each,
following "The requirements conversation" in `hops-reqs/SKILL.md`:

- the problem made precise: `task`, `target` (the label column or how to derive it), `entity`,
  `prediction_time`, `horizon`, `label_maturity`, `generalises_to`, and the business baseline,
  recorded in one write before `requirements.status` leaves `pending` (the validator requires
  `task` from then on):
  `python <slug>/set.py 'requirements.problem={task: classification, target: won, entity: deal_id,
  prediction_time: "daily, 06:00 UTC", horizon: 90 days, label_maturity: 90 days,
  generalises_to: new_periods}'`;
- the features to compute and where (`feature_pipeline`, `streaming`, `on_demand`), preferring
  features that already exist;
- the target: a metric, a number and a direction, and how it is measured;
- the rest of the SLA for the type (`window` and `must_finish_by` for batch, `error_rate_max` and
  `timeout_ms` for real-time);
- `operations` (scheduled with cadence and window, or continuous; the alert receiver), the sizing
  tier (`small` proposed), `budget`, `data_policy`, `models`, and the reviewers for the pull request.
- for batch and realtime, `monitoring` when the UI did not record it: whether predictions log their features (`feature_logging`, proposed true) and what to monitor and alert on (`watch`, in the user's words).
  Never ask it for an example system, which records it only when the user asked.

An example system (`system.example` set) asks nothing here: choose every answer yourself from the
example's story, recorded in `system.yaml`, as a person building a convincing demo would (for
`churn-example`: classification of `churned` within 30 days per customer, PR-AUC 0.6 or better
against a 0.15 prevalence baseline; the `small` tier, the default budget, `data_policy` fixtures
generated). Print the requirements back in a few lines and continue without asking.

When the requirements are met, write each phase's `estimate` (a duration such as `5m` or `1h30m`,
`python <slug>/set.py 'training.estimate=5m'`) from what the requirements say: a pretrained model
that is only downloaded takes minutes, a training round its budget's wall clock, a phase that is
`skipped` nothing. When a phase starts, correct its `estimate` if you know better by then. The
Factory page shows these estimates as the build runs.

When `requirements.reference_code` is set, the system is based on that sample code: read it before
writing any. A path is read where it is (the examples' reference code is under
`/opt/hopsworks-api/python/hopsworks/skills/ml/hops-reqs/references/` in the terminal); a URL is
cloned or downloaded into a scratch directory outside the system's repository. Take what fits the
requirements, adapt the names, and record in `decisions` what came from it and what was changed.

For a new connector the interview recorded as `connected`, the data phase mounts or ingests it. A
RAG agent system (`system_type: agent`, `task: rag`, the help desk example) is built by
`hops-reqs/references/rag-agent.md`, which says what every phase builds for it, from the
reference code in `rag_agent/`: its `reqs` creates the docs directory without asking and asks the
user to upload documents, and its `train` and `infer` follow that page instead of spawning the
training and inference agents. The personalized recommender (`task: ranking`, the `recs-example`)
is built the same way by `hops-reqs/references/recommender.md`, from the reference code in
`recommender/`: its `data` phase downloads the public H&M files instead of generating data, and
its `train` and `infer` follow that page. The credit card fraud detector (the `fraud-example`) is
built by `hops-reqs/references/ccfraud.md` from the book's code at a pinned commit and `ccfraud/`:
its `data` runs the book's generator instead of generating data, its `features` start a Spark
Structured Streaming job, and its `train`, `infer` and monitoring follow that page. The debit card
fraud detector (`task: anomaly`, the `debitcard-example`) is built by
`hops-reqs/references/debitcard.md` from `debitcard/`: its `data` runs that generator instead of
generating data, its `features` run dbt models in Trino daily and start a Spark Structured Streaming
job, and its `train`, `infer` and `app` follow that page. The GIS military infrastructure finder (`task: detection`,
the `gis-example`) has no reference code: every file is written from its requirements and
`hops-reqs/references/gis-detector.md`, which gives each phase's contract, the facts the code
depends on and the tests to write. `data` and `features` are `skipped`, `train` registers a
pretrained detector, and the model is embedded in the app, so `infer` measures the app's detection
latency instead of deploying. Hops Run with Kumo
Tabular (the `run-example`) is built by `hops-reqs/references/kumo-run.md` from `kumo_run/`: `data`
and `features` are `skipped`, `train` registers NVIDIA's pretrained in-context classifier, and
`infer` deploys it and measures the game's decisions. Any other task outside classification, regression and forecasting,
or another agent system, is captured in full and the command stops after `reqs` saying v1 builds
none of it. Print the requirements back and ask to proceed; write `requirements.status: met` and
`system.status: building` on yes.
