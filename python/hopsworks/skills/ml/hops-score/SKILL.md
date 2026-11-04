---
name: hops-score
description: Use when the user wants to score or rank rows, or asks for the chance of an outcome (a deal closing, a customer churning), over data already connected to Hopsworks, with no model to train. Auto-invoke for "score my open deals", "rank these leads", "chance of churn" over CRM, database or feature group data. Runs `hops score`.
---

# Scoring with Kumo-Relational: `hops score`

A pretrained in-context model (`nvidia/Kumo-Relational`) reads labelled rows of one feature
group, the context, and the tables related to it, and returns the chance of the label for the
rows without one. Nothing is trained and no features are written by hand. It runs as a Python
job on CPU in the project, on the `scoring-pipeline` environment, and writes the scores to a
feature group. Use a trained model (hops-train, hops-batch-inference) when the user wants one.

## Input contract

- Cached feature groups only. A source is copied in first: `hops fg ingest <name> --connector <source> --table <table> --primary-key <cols>`. An external group is refused, and the message names that command.
- One entity group: one row per entity, a primary key (composite is fine) and a target column. Rows with a null target are scored; labelled rows are the context. `--where "<filter>"` scores the rows it picks instead, and the context is every other labelled row.
- Related groups are named only by links, never matched by column name. Either `--fv <feature view>` (its joins are the links) or `--link deals.contact_id=contacts.id`, repeatable; a composite key is `--link a.k1+a.k2=b.k1+b.k2`. The right side of a link must be unique on its key. A group is at most 2 hops from the entity. A group with no link, or two links for one pair, is refused with the candidates listed.
- Time: `--time <group>.<column>` for each table that has a time (`--time deals.created_at --time activities.occurred_at`), and `--cutoff deals.closed_at`, the time each label became known. Rows of a time table after a row's cutoff are never used for it; query rows use the run's start. Without `--time` the output says the scores may use rows from after each label's time: give both whenever the data has them.
- A boolean or 0/1 target needs no `--positive`; any other needs `--positive <value>`.

```bash
hops score deals --entity deals --target won \
  --link deals.contact_id=contacts.id --link activities.deal_id=deals.id \
  --time deals.created_at --time activities.occurred_at --cutoff deals.closed_at --schedule
```

## Caps

Counted by Trino on the offline tables before anything is read; over a cap the run refuses
with the counts and the cap.

| Cap | Value |
| --- | --- |
| `--context-rows` | 1,000 (a larger value refuses) |
| `--query-rows` | 10,000 per run, scored in batches of 1,000 |
| Related rows / bytes after projection, counted on the rows reachable from the selected rows | 100,000 / 1 GB |
| Entity group bytes after projection | 256 MB |

Related rows are read only for the selected context and query entities. The job has 4 cores
and 6144 MB. A project without the `scoring-pipeline` environment is refused.

## Run, schedule and re-run

`hops score <name> ...` deploys the job `kumo-score-<name>`, runs it and waits as `hops job run --wait`
does. `hops score <name>` again re-runs it. One run per system: while the job has an execution
that has not ended, the verb refuses and names it. `--schedule` alone runs daily at a time
derived from the project and the name; `--schedule "<quartz cron>"` is kept as given and must
not fire more often than hourly. A scheduled run is not waited on.

## Reading the current run

Each run writes all its rows to `<name>_scores` in one insert (the entity key, `run_id`,
`scored_at`, `score`, `predicted_label`, `model_version`), then its row to `<name>_score_runs`.
A run is current only once its runs row exists. The current ranking is the scores of the
latest runs row; the latest `run_id` in the scores group is not it. Use:

```bash
hops score <name> --show [--top 20]
```

An entity absent from the current run (a deal closed since) has no current score. When every
row has closed the current run is empty, and `--show` says that run scored none. Rows of a run
that failed after its insert stay in the group as history and are never current.

## Teardown

`hops score <name> --delete` removes the job with its schedule and both feature groups, after
listing them. The input groups and the environment stay.
