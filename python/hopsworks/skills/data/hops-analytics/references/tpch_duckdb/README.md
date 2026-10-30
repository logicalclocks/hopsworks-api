# TPC-H Q8 with DuckDB and dbt

The `tpch-duckdb` blueprint of the `analytics-pipeline` factory: one Python job
that generates TPC-H at scale factor 1 in DuckDB, runs TPC-H Q8 (national market
share) as a DAG of dbt models in DuckDB, and writes the result to offline Delta
feature groups, with a separate program that builds its Superset dashboard.

| File | What it is |
| --- | --- |
| `tpch_pipeline.py` | the job: dbgen, `dbt build`, the feature group writes, the execution graph |
| `tpch_q8/` | the dbt project: a staging model per TPC-H table, one model per node of the DAG |
| `tpch_dashboard.py` | the Superset dashboard over the two feature groups |
| `tests/test_tpch_q8.py` | the models against DuckDB's own `PRAGMA tpch(8)` at scale factor 0.1 |

Copy the directory into the system and change nothing else: it is tested as it is.

## The DAG

The models follow the DAG node for node. Each `stg_<table>` is a table's
`select_data` (its needed columns). `america` is the AMERICA region,
`american_nations` its nations, `american_customers` and `american_orders` theirs.
`order_data_augmented` joins each American order's lines with the part's type
and the supplier's nation. `order_data_filtered` keeps the lines ordered between
the `start_date` and `end_date` vars and of type `p_type` (TPC-H Q8's 1995-01-01,
1996-12-31 and ECONOMY ANODIZED STEEL by default; `--vars` changes them).
`processed` adds `volume` (price after discount), `o_year` and `brazil_volume`.
`brazil_volume_by_year` is the final output: Brazil's share of the American market
per year. `america_volume_by_nation_year` splits the same market by every supplier
nation for the dashboard. Every model is a view, so DuckDB plans the whole query at
once and holds no intermediate table; only the two outputs are tables. The data
tests check the keys, that a share is between 0 and 1, and that the nations add up
to the American market.

## Build

```bash
python -m pytest tests                    # in the dbt-pipeline environment, about 10 s
hops files upload tpch_duckdb <system dir>
P=hdfs:///Projects/<project>/<system dir>/tpch_duckdb
hops job deploy <slug>-pipeline $P/tpch_pipeline.py --env dbt-pipeline --args "--system <slug>"
```

The stock `dbt-pipeline` environment ships DuckDB, dbt-core, dbt-duckdb and
`dbt_run_graph`; nothing is installed. Then give the job 1 core and its memory
(below), and run it:

```python
job = hopsworks.login().get_job_api().get_job("<slug>-pipeline")
job.config["resourceConfig"]["memory"] = 1024
job.config["resourceConfig"]["cores"] = 1
job.save()
```

```bash
hops job run <slug>-pipeline --wait
```

A run takes about a minute: dbgen writes the eight tables (6,001,215 line items)
into a DuckDB file in the job's scratch directory, `dbt build` runs the models and
tests in a subprocess, and the outputs are written to
`tpch_brazil_market_share` (`o_year`, `mkt_share`, `brazil_volume`,
`america_volume`; primary key `o_year`) and `tpch_america_volume_by_nation`
(`o_year`, `supplier_nation`, `volume`), both offline Delta with `year_start` as
the event time and statistics off. Brazil's share is 0.0344 in 1995 and 0.0415 in
1996. The dbt artifacts and the execution graph land in
`Resources/dbt_runs/<job>/<execution>`. Every run recomputes everything, so a
schedule needs no window; record `window: full` and `backfill: {last: none}`.

## Memory

DuckDB runs with `--memory-limit 300MB`, one thread and insertion order not
preserved, and spills to the scratch directory beyond the limit. Measured as a
Hopsworks job with 1 core:

| Job memory | Result |
| --- | --- |
| 2048, 1024, 768, 640, 512 MB | succeeds in about a minute |
| 448, 384 MB | OOMKilled during dbgen |

dbgen dominates: its generation buffers sit outside DuckDB's memory limit, and the
runner peaks at about 620 MB resident while it runs (part of that is file-backed
pages the kernel can reclaim, which is why 512 MB still passes). `dbt build`
runs afterwards, in its own process, and peaks at about 545 MB. The job prints
both peaks at the end of every run. Give it 1024 MB: above both peaks with room
for the client and page cache, and twice the lowest memory that passed, so a run
never fails on it. A larger
`--memory-limit`, more threads (2 cores) or a larger scale factor need more.

## Dashboard

```bash
hops job deploy <slug>-dashboard $P/tpch_dashboard.py --env python-feature-pipeline --run --wait
hops job logs <slug>-dashboard --stdout       # the dashboard's URL
```

It creates, or updates in place, the Superset dashboard "TPC-H Q8: Brazil's share
of the American market": Brazil's share in the last year, its share by year, the
American market by supplier nation and year (stacked), and the yearly figures.
Record it in `dashboard.dashboards`.
