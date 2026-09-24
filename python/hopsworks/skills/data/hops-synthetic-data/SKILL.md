---
name: hops-synthetic-data
description: Generate seeded synthetic data into feature groups with Polars, as batch tables or a live stream of events written by a continuously running job. Auto-invoke when the user wants test data, demo data or synthetic data for a feature group or an ML system ("generate test data for X", "I have no data yet"). Input a story and a schema; output feature groups with rows and, for events, a running writer job.
---

# Synthetic data

Generate data that has a known story, so a pipeline or an ML system can be built
and trained before a real source is connected. The generator is a program of
the system, seeded and run as a Hopsworks job, never on the laptop.

## Contract
- **Input:** the shape (batch tables or events), the entity and its
  cardinality, the event time, the columns with types and rough distributions,
  and the **story**: which columns carry the signal the target depends on, and
  how strongly. Inside `/hops-build` these are a `kind: synthetic` entry of
  `requirements.data_sources` and its `data.<source>` block.
- **Output:** feature groups with rows; for events, an online-enabled feature
  group fed by a running `<slug>-events` job.
- **Pre-condition:** the volume and rate are confirmed against the sizing tier.

## Smoke-test (cheap pre/post-flight)
```bash
hops fg preview usage_events --n 5          # rows landed (offline)
hops fg preview usage_events --n 5 --online # the stream is fresh (online)
hops job history <slug>-events              # the live writer has a RUNNING execution
```

## Ask the user (only when state is ambiguous)
- Batch or events: batch for a batch system or an entity table with history;
  events for a realtime system, a feature `computed_in: streaming`, or a story
  that is a stream of events over entities.
- The story, in one or two sentences. It is a requirement: it bounds what any
  model can achieve on this data.
- A volume or rate above the tier's default: confirm in one line before generating.

## The rules
- **Seeded.** The same seed gives the same data on any cluster; a live tick is
  seeded from its index, so a restart continues from now with no state.
- **The story is code.** Signal columns are generated with the declared
  dependency plus noise; the target's positive rate follows
  `requirements.targets.prevalence`. No column is a function of the target the
  real world would not have at prediction time.
- **Polars, in `python-feature-pipeline`.** The columns and the relational steps
  that shape the story (joins between entities and events, window aggregates)
  are Polars; the base ships it, so the generator runs there and no environment
  is cloned.

## Sinks
- **Batch:** one offline feature group per table (the entity table, and an event
  history when the story has one), named as the real source would be, with
  `event_time` set and a description saying it is synthetic and which job wrote
  it. One run, `<slug>-data-backfill`, with `--mode backfill --from --to`.
- **Events:** one **online-enabled** feature group (`online_enabled=True,
  stream=True`), primary key the **event id** (an entity key keeps only the
  latest row per entity online), a `ttl` so the online store forgets old events
  (default seven days), and `offline_backfill_every_hr` so materialization to the
  offline store runs on a schedule (default hourly) rather than per insert.
  First a `--mode backfill` run writes the history the training phase needs,
  finalises its multi-part insert, runs the materialization job and waits; the
  rows count as training data only once `hops sql` counts them offline. Then the
  **same program** in `--mode live` is deployed as `<slug>-events` and left
  running: every `tick_s` it writes `rate_per_s x tick_s` events with
  `multi_part_insert`, which starts no materialization job per batch.

## Program shape

[references/generator.py](references/generator.py) is the skeleton, copied into
the system as `src/<slug_pkg>/synthetic_data.py`: the bundle prelude,
`entities(...)` and `events(...)` carrying the story, `tick(...)` for live mode,
and a `main` that reads the source's `data.<source>` block (`generator.seed`,
`writes`, `live`, `backfill.rows` for the size of the history, and an optional
`entities` sink for the entity table). The history is sized by row count, not
by the live rate: five events a second over three months is 40 million rows.

```bash
hops job deploy telco-churn-data-backfill telco-churn/src/telco_churn/synthetic_data.py --env python-feature-pipeline \
  --args "--bundle Resources/telco-churn/runs/data-backfill-1/bundle.tar.gz --mode backfill --from 2026-06-01 --to 2026-09-22" --run --wait --overwrite
hops sql --catalog delta --schema <project>_featurestore "SELECT count(*), max(ts) FROM usage_events_1"   # offline rows, after materialization
hops job deploy telco-churn-events telco-churn/src/telco_churn/synthetic_data.py --env python-feature-pipeline \
  --args "--bundle Resources/telco-churn/runs/data-live-1/bundle.tar.gz --mode live" --run --overwrite
```

## Sizing

Defaults are modest and only the user raises them. The tier is
`requirements.sizing.tier`:

| Tier | Batch, per table | Events | Online store at a 7-day TTL | Offline growth |
| --- | --- | --- | --- | --- |
| small (default) | 10,000 entities, 100,000 history rows | 5 events/s over 2,000 entities, 10 s ticks | about 3 million rows | about 100 MB a day |
| medium | 100,000 entities, 1 million rows | 50 events/s over 20,000 entities, 10 s ticks | about 30 million rows | about 1 GB a day |
| large | the user's numbers | the user's numbers, 1 s ticks allowed | rate x TTL | as computed |

The numbers assume rows of about 200 bytes; recompute the right-hand columns
from the declared columns and state them before the stream starts. A live writer
costs one Python job's pod for as long as it runs, plus one materialization run
per schedule. Keep `tick_s` at ten seconds or more below the large tier: a tick
is a Kafka batch, and a smaller one buys nothing.

## Operating the stream
```bash
hops job history telco-churn-events   # RUNNING means live
hops job stop telco-churn-events      # the user's call
hops job run telco-churn-events       # restart; continues from now
```
Hopsworks jobs have no restart policy: `/hops verify` and `/hops status` report
a writer that died, and `hops job run` starts it again.

## Tests
Unit, offline: a fixed seed gives the same frame twice; columns and types match
the declared schema; the target's rate is within tolerance of the declared
prevalence; the story holds on a sample (the signal columns move with the target
in the declared direction); no column reproduces the target; one live tick
yields `rate_per_s x tick_s` rows inside the tick. Integration: a one-day
backfill into `<ident>_test_<run_id>` lands the expected rows with unique keys;
three live ticks into an online test group are readable online; both are deleted.

## Next Steps
- Mount or ingest a real source instead: **hops-data-sources**.
- Build features over the synthetic groups: **hops-features**, **hops-fg**.
