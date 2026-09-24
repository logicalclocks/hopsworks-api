# `system.yaml`: rules, status vocabulary and schema

The spec of record for one ML system, at `<repo>/<slug>/system.yaml`. The
orchestrator reads state from this file only; each phase writes its own block.
A complete worked example, a batch churn system that finished every phase, is
[example-system.yaml](example-system.yaml); `tests/unit/test_system_yaml.py`
from the [system template](system_template/) validates a file against the rules
below and passes on that example.

## Rules

1. **One phase, one section.** A phase writes its own section, appends to `decisions`, and may
   update `system.status`. Every other line is preserved byte for byte, and the file must stay
   valid YAML. That is what makes the file trustworthy across runs.
   Every phase has a section, in the order the phases run: `requirements`, `data`,
   `features`, `training`, `inference`, `app`, `verify`, each with a `status`, so the
   file alone says how far the system got and what each step decided and measured. Each block
   also records `started` and `finished`, and `system.progress` the current position and the
   remaining estimate (Progress and estimates, below).
   Write a field with `python <slug>/set.py 'dotted.key=<yaml value>'` (`key+=value` appends to a
   list): it validates and renames into place, so an invalid write never replaces the file. While
   `requirements.status` is `pending` the file is a draft, and `task`, `system_type` and `sla` may
   still be missing, since the `/hops-ml` interview records them one answer at a time.
2. **Only as wordy as needed.** A field is one line unless it genuinely needs a `>` block. A
   `note` is one sentence. A decision is one line. The reader has the pipeline code next to the
   file; the YAML says what and why, the code says how.
3. **Numbers, not adjectives.** `targets` and `sla` hold a number, a direction, and how it is
   measured. "Good accuracy" is rejected at the requirements phase.
4. **Nothing under `measured` or `created` that was not verified with the `hops` CLI in the
   same run.** A claim without a check is not recorded.
5. **No secrets.** An LLM key is referenced by the name of the Hopsworks project secret that
   holds it.
6. **Naming.** Three identifier flavours from one slug, because Hopsworks object types have
   different character rules: `ident` (snake_case) for feature groups, feature views and
   models; `slug` (hyphens) for jobs and apps; `alnum` for deployments and agents.
7. **Validated on every write.** `tests/unit/test_system_yaml.py`, shipped with the system
   from the template, checks the file against this schema: `schema_version`, the status vocabulary, one `sla`
   block, one route per data source, the shape of every `runs` and `measured` row, and the
   identity fields on every piece of evidence. It runs with the unit tests, and a phase writes
   the YAML to a temporary file, runs that test against it, and renames it into place, so a
   half-written or invalid file never replaces a valid one.
8. **One invocation at a time.** `/hops` takes `<slug>/.hops.lock` (holder, host, time) for
   the whole invocation and refuses to start while another holds it, so two sessions cannot
   interleave writes. A lock older than a day with no running execution behind it is reported
   and may be taken over.
9. **Intent before effect, evidence after.** A row for a job run or a model registration is
   written with `state: submitted` before the remote call, updated to `running` with the
   execution id, and to `finished` or `failed` with the result, so a crash between the effect
   and its record is found by reconciliation on resume instead of producing a duplicate. Code
   commits and evidence commits are separate: the program is committed before it runs
   (`[<slug>] train run <n>: <what changed>`), its result after (`[<slug>] train run <n>:
   result`), and a revert targets the recorded code commit, never `HEAD`.

## Status vocabulary and transitions

Every phase block has a `status`. Satisfied: `met`; `skipped` (the phase does not apply, such
as `training` for an agent system without a model, or `app` declined); `accepted` (the user
waived the target or SLA; the block records what was accepted and a `decisions` line says so).
Not satisfied: `pending`, `running`, `unmet`, `stale`. `verify` has `pass | fail`, is rerun by
every resume, and is never a gate.

| Change, or failed claim | Marked `stale` |
|---|---|
| `requirements` edited (any field but `status`) | every later phase |
| a data source's status or a generator's parameters changed | `data` onwards |
| an entrypoint, transformation or `evaluate.py` commit differs from the one its evidence names | the phase owning the file, and every later phase |
| feature view or training dataset version changed | `training` onwards |
| `training.model` changed (a new accepted or promoted version) | `inference` onwards |
| a deployment or job the YAML names is missing, stopped, unscheduled or failing | the phase that owns it |
| evidence past its expiry (benchmark and freshness claims: seven days by default) | the phase that owns it |

## Schema

```yaml
schema_version: 1

system:
  name: Telco churn call list
  slug: telco-churn
  target: {cluster: https://hopsworks.acme.internal, project: skillstest, stage: development}   # the autonomous path runs only against stage: development
  repo: {url: https://github.com/acme/ml-systems, host: github.com, default_branch: main, branch: hops/telco-churn, pr: 12}
  created: 2026-09-22
  versions: {hopsworks: 4.6.0, cli: 4.6.0, skills: 2026-09-22, protocol: hops-train/references/autoresearch.md@1133d9c}
  progress: {phase: train, done: [reqs, data, features], now: "run 4 of 5, execution 1107 at 6m of 10m",
             remaining_estimate: 51m, estimate_basis: "runs so far; defaults for infer, app, verify"}
  status: draft | building | verified | deployed

requirements:                         # owner: reqs
  description: >                      # two or three sentences a stakeholder would recognise
  problem:
    task: classification | regression | forecasting   # v1 builds these; ranking | anomaly | rag | agentic are captured and stop at reqs (decision 20)
    target: churn                     # null for agentic systems
    entity: customer_id
    prediction_time: monthly, 1st, after the billing snapshot lands
    horizon: 1 month
    label_maturity: 1 month           # how long after event_time a label is final; younger rows are in no split
    generalises_to: new_periods | new_entities   # new_periods: time-ordered split; new_entities: split grouped by entity
  system_type: batch | realtime | agent
  data_sources:                       # one of the three routes per source; the data phase turns every status into present
    - {name: telco_customers, kind: feature_group, version: 1, grain: one row per customer, status: present}
    - {name: billing, kind: datasource, type: snowflake, connector: acme_snowflake, table: BILLING.INVOICES,
       grain: one row per invoice, event_time: invoice_date, status: needs_connection,
       needs: [account url, user, database, schema, warehouse; password by stdin or HOPSWORKS_DS_SNOWFLAKE_PASSWORD]}
    - {name: tariffs, kind: file | url, location: https://.../tariffs.csv, grain: one row per plan, status: needs_download}
    - {name: usage_events, kind: synthetic, shape: events, grain: one row per call, entity: customer_id,
       event_time: ts, status: needs_generation}
    #  status: present | connected | needs_connection | needs_download | needs_generation
    #  connected = the connector exists but the table is not yet a feature group; present = the pipelines can read it
  sizing: {tier: small}               # small | medium | large: what the cluster can carry; small unless the user says otherwise
  budget:                             # the resource envelope every agent works inside; nothing below raises it
    training: {max_runs: 5, wall_clock: 60m, per_run: 10m, rounds: 3, import_timeout: 20m}
    inference: {max_attempts: 5, wall_clock: 60m, max_replicas: 4, max_memory_mb: 4096, max_cores: 2, gpu: false}
    operations: {streams: 1, scheduled_jobs: 4}
  data_policy:
    fixtures: generated | sanitised | real   # real requires export_ok
    export_ok: false                  # may real rows leave the cluster: into fixtures, the repository, a PR body, a transcript
    log_fields: [customer_id, prediction, score]   # what the inference pipeline may log; raw inputs and free text must be listed to be logged
  models:                             # where a pretrained model may come from
    sources: [huggingface]
    licences: [apache-2.0, mit, bsd-3-clause]
    token_secret: hf_token            # Hopsworks project secret holding the Hub token
  features:                           # what must be computed, and where
    - {name: tenure_months, computed_in: feature_pipeline | streaming | on_demand,
       from: telco_customers, note: ...}
  targets:                            # the training agent's goal, judged on the test split
    metric: pr_auc
    target: 0.65
    direction: max
    baseline: {value: 0.43, metric: precision_at_500, note: the current month-to-month rule at 500 calls}   # a score on its own metric
    prevalence: 0.26                  # positive rate; distinct from the baseline
    how_measured: held-out temporal test split, never used for selection
    secondary: [recall_at_500]
  sla:                                # the inference agent's goal, keyed by system_type
    batch:    {rows_per_run: 5200, cadence: monthly, window: closed billing month, must_finish_by: 06:00 UTC}
    realtime: {p99_ms: 50, throughput_qps: 100, error_rate_max: 0.001, timeout_ms: 500}
    agent:    {p99_ms: 3000, throughput_qps: 5, eval: {metric: answer_accuracy, target: 0.85,
               eval_set: evals/telco_agent_evals.parquet, scorer: evals/score.py}}   # captured; agent systems do not build in v1
  operations:                         # how the pipelines run once built (hops-job: Windows and backfill, Continuous jobs)
    features:  {run: scheduled, engine: polars, cadence: "0 0 2 1 * ?", window: previous calendar month, backfill_from: 2025-01-01}
    #          {run: continuous, engine: spark_streaming, source: usage_events, trigger: 30s}
    training:  {run: scheduled, cadence: "0 0 3 1 * ?", promote: when the test metric holds on the new split}
    inference: {run: scheduled, cadence: "0 0 4 1 * ?"}   # batch; a realtime system runs a deployment instead
    ordering: cadence | dag           # cadence gaps sized from measured durations, or one Airflow DAG per system
    alerts: {receiver: ml-oncall, on: [job_failed, freshness_missed, retrain_rejected]}
  consumers: ui | api | both
  monitoring: predictions and inputs logged to the prediction feature group; drift on the FV
  open_questions: []                  # non-empty blocks the data phase
  started: 2026-09-22T09:02Z          # every phase block records started and finished; /hops status derives durations and estimates
  finished: 2026-09-22T09:13Z
  status: pending | met

data:                                 # owner: data; one entry per source that was not already present
  billing:
    connector: {name: acme_snowflake, type: snowflake, created: 2026-09-22}
    mounted_as: {feature_group: billing_invoices, version: 1, external: true, event_time: invoice_date}   # or ingested: {job: telco-churn-ingest-billing, feature_group: ...}
  tariffs:
    landed: {path: Resources/telco-churn/data/tariffs.csv, rows: 42, by: hops files upload}
  usage_events:
    generator: {file: src/telco_churn/synthetic_data.py, seed: 20260922, story: "calls per customer follow tenure and plan; churners' volume decays over the last 60 days"}
    environment: {name: python-feature-pipeline}   # the base ships polars; nothing is cloned
    writes: {feature_group: usage_events, version: 1, primary_key: event_id, event_time: ts, online: true, ttl: 7d, offline_backfill_every: 1h}
    backfill: {run_id: data-backfill-1, rows: 100000, from: 2026-06-01, to: 2026-09-22, job: telco-churn-data-backfill, execution: 1031,
               materialized: {execution: 1032, offline_rows: 100000}}   # counted through hops sql after materialization finished
    live: {rate_per_s: 5, entities: 2000, tick_s: 10, job: telco-churn-events, execution: 1033}   # events only; the execution runs until stopped
    tests:
      unit: tests/unit/test_data.py
      integration: {file: tests/integration/test_synthetic_data.py, job: telco-churn-tests, environment: python-feature-pipeline}
      last_run: {when: 2026-09-22T09:40Z, run_id: tests-data-1, commit: 41c0f2e, unit: "6 passed", integration: "3 passed", test_objects_deleted: true}
  status: pending | met | stale

features:                             # owner: features
  status: pending | met | unmet | stale
  pipelines:
    - name: telco_churn_features
      run: scheduled | continuous     # from requirements.operations.features
      engine: pandas | polars | pyspark | spark_streaming
      environment: {name: python-feature-pipeline}     # a base had everything; nothing cloned
      reads: [telco_customers, billing_invoices]
      transformations: [tenure bucket, charges ratio, service count]   # MITs only
      writes: {feature_group: telco_churn_customers, version: 1, online: false}
      job: {name: telco-churn-features, type: python, schedule: {cron: "0 0 2 1 * ?", window: {start_offset_s: -2678400, end_offset_s: 0}, catchup: false, max_active_runs: 1},
            backfill: {from: 2025-01-01, to: 2026-09-01, execution: 1042}, alert: ml-oncall}
      #     continuous: job: {name: telco-churn-usage-stream, type: pyspark, checkpoint: Resources/telco-churn/checkpoints/usage, trigger: 30s, execution: 1050, alert: ml-oncall}
      benchmark: {file: benchmarks/benchmark_features.py, last_run: {run_id: bench-features-1, commit: 41c0f2e, execution: 1043, kind: full_window,
                  rows_per_s: 9400, full_window_s: 41, fits_before_inference: true}}
      tests:
        unit: tests/unit/test_features.py
        integration: {file: tests/integration/test_feature_pipeline.py, job: telco-churn-tests, environment: python-feature-pipeline}
        last_run: {when: 2026-09-22T10:14Z, run_id: tests-features-1, commit: 41c0f2e, unit: "14 passed", integration: "6 passed", test_objects_deleted: true}
      status: pending | met | unmet | stale

training:                             # owner: train (the training agent); budget is requirements.budget.training
  required: true                      # false for an agent system with no model
  environment: {name: telco-churn-train-env, base: pandas-training-pipeline, requirements: envs/training-requirements.txt,
                transformation_libraries: [scikit-learn==1.5.2]}   # imported by the feature view's custom transformations
  feature_view: {name: telco_churn_fv, version: 1, label: churn,
                 transformations: [one_hot(contract), min_max(charges)]}   # MDTs, statistics fitted on the train part
  eda: eda.md                         # first round only; later rounds and retrains run the short checks
  leakage: [months_to_contract_end]   # column names eda.md found leaky; excluded from every run and checked by retrain
  autoresearch: autoresearch.md       # optional: the loop protocol the training agent follows; absent = the one shipped with hops-train
  pretrained: {searched: true, candidates: [], chosen: none, note: tabular churn on private features; no Hub model fits the task}
                                      # or candidates: [{model: hf:owner/repo, revision: 8f3a1c9, licence: apache-2.0, run_id: train-1-1}], chosen: hf:owner/repo@8f3a1c9
  split: {kind: temporal | grouped | random, validation_fraction: 0.15, test_fraction: 0.15, start: 2025-01-01, label_cutoff: 2026-08-22,
          note: event_time present and generalises_to new_periods, so time-ordered with test last}
  harness: {evaluate: src/telco_churn/evaluate.py, commit: 3f9e2a1, training_dataset_version: 1}   # frozen before the first run; travels in every run bundle
  runs:                               # research mode: every run registers a version of telco_churn_research; the row exists before the job is submitted
    - {run_id: train-1-1, n: 1, attempt: 1, commit: 8c1d0e4, bundle: runs/train-1-1, execution: 1101, state: finished,
       model: {name: telco_churn_research, version: 1}, algorithm: xgboost, changed: baseline, pr_auc_validation: 0.58, status: keep, met: false}
    - {run_id: train-2-1, n: 2, attempt: 1, commit: b72f9aa, bundle: runs/train-2-1, execution: 1104, state: finished,
       model: {name: telco_churn_research, version: 2}, algorithm: xgboost, changed: max_depth 6->3, pr_auc_validation: 0.55, status: discard, met: false}
    - {run_id: train-3-1, n: 3, attempt: 1, commit: e04c7d2, bundle: runs/train-3-1, execution: 1107, state: finished,
       model: {name: telco_churn_research, version: 3}, algorithm: xgboost, changed: +charges_ratio, pr_auc_validation: 0.66, status: keep, met: true}
  acceptance:                         # accept mode on the best kept run: refit on train + validation, scored once on the test part
    - {run_id: train-accept-1, commit: e04c7d2, bundle: runs/train-accept-1, execution: 1109, state: finished,
       model: {name: telco_churn_model, version: 1}, pr_auc_validation: 0.66, pr_auc_test: 0.65, met: true}
  model: {name: telco_churn_model, version: 1, framework: xgboost, commit: e04c7d2}   # the approved version inference reads; research versions never live under this name
  retraining: {job: telco-churn-train, mode: retrain, schedule: {cron: "0 0 3 1 * ?"}, alert: ml-oncall,
               promote: register a new telco_churn_model version only when pr_auc_test >= 0.65 on the new split; else keep serving version 1 and alert}
  tests:
    unit: tests/unit/test_training.py
    integration: {file: tests/integration/test_training_pipeline.py, job: telco-churn-tests, environment: telco-churn-train-env}
    last_run: {when: 2026-09-22T11:02Z, run_id: tests-training-1, commit: e04c7d2, unit: "9 passed", integration: "4 passed", test_objects_deleted: true}
  status: pending | running | met | unmet | accepted | stale | skipped

inference:                            # owner: infer (the inference agent); budget is requirements.budget.inference
  mode: batch | realtime | agent
  environment: {name: telco-churn-train-env, reused_from: training}   # batch inference in the training environment: same libraries, no skew
  # a realtime deployment instead clones an inference base with the same transformation_libraries versions
  # one of:
  batch:    {reads: {feature_view: telco_churn_fv, window: closed billing month},
             writes: {feature_group: telco_churn_predictions, version: 1},
             job: {name: telco-churn-inference, type: python, schedule: {cron: "0 0 4 1 * ?", window: {start_offset_s: -2678400, end_offset_s: 0}}, alert: ml-oncall}}
  realtime: {deployment: telcochurnpredictor, model_version: 1, replicas: 2, batched_lookups: false,
             latency_breakdown: {online_lookup_ms: 5, model_ms: 8, overhead_ms: 4}}
  agent:    {deployment: telcochurnagent, llm: {endpoint: ..., model: ..., api_key_secret: llm_key},
             rag: [telco_churn_fv], trace_logging: every step}
  benchmark:                          # the program that produces `measured`
    file: benchmarks/benchmark_inference.py
    job: telco-churn-benchmark
    protocol: {load: closed_loop, offered_qps: 100, concurrency: 8, duration_s: 60, warmup_s: 10, timeout_ms: 500, min_requests: 5000,
               location: in_cluster_job, errors: counted, frozen_at: 9a1b2c3}   # frozen for the tuning round like the training harness
    params: {duration_s: 60, qps: 100, entities: 500}   # realtime; batch uses {window: ...}; agent uses {eval_set: ...}
  measured:                           # one line per benchmark run, imported by the orchestrator from the run's result.json
    - {n: 1, run_id: bench-1, commit: 9a1b2c3, execution: 1120, kind: full_window, rows_per_s: 1800, window_s: 2.9, fits_window: true}   # batch
    - {n: 1, run_id: bench-1, commit: 9a1b2c3, execution: 1120, model_version: 1, deployment_config: 5d2e..., offered_qps: 100, completed_qps: 99.6,
       p50_ms: 31, p99_ms: 74, error_rate: 0.0, met: false, changed: baseline}   # realtime
    - {n: 2, run_id: bench-2, commit: 9a1b2c3, execution: 1124, model_version: 1, deployment_config: 8c7f..., offered_qps: 100, completed_qps: 100,
       p50_ms: 22, p99_ms: 46, error_rate: 0.0, met: true, changed: replicas 1->2}
  tests:
    unit: tests/unit/test_inference.py
    integration: {file: tests/integration/test_inference_pipeline.py, job: telco-churn-tests, environment: telco-churn-train-env}
    parity: {entities: 200, at: 2026-09-22T11:30Z, model_version: 1, tolerance: 1e-6, max_mismatch: 0}   # same rows at a fixed timestamp, batch path == online path
    last_run: {when: 2026-09-22T11:40Z, run_id: tests-inference-1, commit: 9a1b2c3, unit: "7 passed", integration: "5 passed", parity: "200/200", test_objects_deleted: true}
  status: pending | running | met | unmet | accepted | stale

app:                                  # owner: app
  wanted: true | false
  kind: dashboard | query_ui | chat
  name: telco-churn-app
  environment: {name: python-app-pipeline}
  url: https://...
  status: pending | met | skipped | stale

verify:                               # owner: verify; the only thing it writes
  last_run: 2026-09-22T12:05Z
  commit: 9a1b2c3                     # the pushed head the claims were checked against
  claims: 24
  failed: 0
  stale: []                           # phases marked stale by this run, e.g. [inference]
  expired: []                         # evidence past its expiry, e.g. [inference.measured[2]]
  failures: []                        # e.g. [{claim: inference.measured meets sla.realtime.p99_ms, owner: inference, declared: 50, observed: 74}]
  tests: {unit: "36 passed", integration: not run}   # integration and benchmark only with /hops verify integration|benchmark
  status: pass | fail

decisions:                            # append-only, one line each, who made it and why
  - {phase: reqs,  by: user,   what: system_type batch,       why: predictions are worked monthly}
  - {phase: reqs,  by: claude, what: temporal split,          why: event time present, generalises to new periods}
  - {phase: data,  by: user,   what: synthetic usage events at 5/s, why: no call data yet; small tier}
  - {phase: train, by: claude, what: added charges_ratio,     why: round 1 unmet, agent named it}
  - {phase: infer, by: claude, what: replicas 1->2,           why: p99 74ms vs 50ms target, within max_replicas 4}
  - {phase: app,   by: user,   what: dashboard wanted,        why: retention team reads a list}
```

Blocks that do not apply to the chosen `system_type` are omitted, not left empty. The
`sla` key holds exactly one of `batch`, `realtime`, `agent`; the schema above shows all three
only to document them.

## Writing the file

A phase never edits `system.yaml` in place. It writes the new content to a
temporary file, validates it, and renames it over the old one, so a half-written
or invalid file never replaces a valid one:

```bash
python <slug>/tests/unit/test_system_yaml.py /tmp/system.yaml.new && mv /tmp/system.yaml.new <slug>/system.yaml
```

The lock is `<slug>/.hops.lock`, a one-line YAML mapping written when an
invocation starts and removed when it ends:

```yaml
{holder: "claude-code session 5837752b", host: jim-laptop, since: 2026-09-22T09:02Z}
```

A second invocation that finds the file refuses to start and prints it. A lock
older than a day whose holder has no running execution in `system.progress.now`
is reported, and the user may tell the command to take it over.

## Progress and estimates

`system.progress` is written by the orchestrator at every event: `phase` (the
current phase), `done` (the satisfied phases), `now` (what runs, with its
execution id and elapsed time against `per_run`), `remaining_estimate` and
`estimate_basis`. `status.py` from the template renders the table from those
fields and each block's `started` and `finished`, without a session or the lock:

```
phase      status    started  took   remaining   now
reqs       met       09:02    11m
data       met       09:13    6m
features   met       09:19    14m
train      running   09:33    38m    ~22m        run 4 of 5, execution 1110 at 6m of 10m
infer      pending                   ~15m        5 attempts x ~3m
app        pending                   ~10m
verify     pending                   ~3m
3 of 7 phases done; about 50m left of ~1h59m; ends about 11:01 unless something escalates
```

Estimates, in order of preference:

- A phase that ran before (a rerun or a resume) is estimated from its own `started` and `finished`.
- A training round in progress: remaining runs (`max_runs` less the finished and failed rows) times the average run so far, capped by what is left of `wall_clock`.
- An inference round in progress: remaining attempts times the average attempt so far.
- Defaults before anything has run: reqs 10 min; data 5 min per existing feature group and 20 min per new connector, file or synthetic source; features 15 min per pipeline; train the budget's `wall_clock`; infer 3 min per attempt; app 10 min (none when `app.wanted` is false); verify 3 min.

An estimate is always labelled as one and excludes escalations, review rounds and a raised budget.
