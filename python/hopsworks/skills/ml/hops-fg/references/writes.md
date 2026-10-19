# Large writes, statistics and materialization

## Multi-part insert

When inserting many small batches (e.g., streaming or chunked processing), use multi-part insert to avoid overhead per batch:

**Pattern 1: context manager (preferred):**
```python
with fg.multi_part_insert() as writer:
    for batch_df in data_batches:
        writer.insert(batch_df)
# Automatically finalized when context exits
```

**Pattern 2: manual:**
```python
for batch_df in data_batches:
    fg.multi_part_insert(batch_df)

fg.finalize_multi_part_insert()  # blocking: waits for all rows to transmit
```

After finalizing, run the materialization once (see **Materialization** below and in the parent skill).

## Memory and OOM

Before writing a feature pipeline, estimate whether the data fits in RAM:

```
Memory ≈ rows × columns × avg_bytes_per_value × overhead_factor
```

- Numeric (int/float): ~8 bytes; string: ~50-200 bytes (varies)
- Overhead factor: ~2-3x (Polars/Pandas internal bookkeeping, intermediate results)

**Example:** 1M rows × 40 columns × 8 bytes × 3 ≈ 960 MB, fits in most environments.

If the data does not fit, or a pipeline OOMs (usually from reading every source FG at once, or rolling-window intermediates):

1. **Read in slices** by event time: `fg.read(start_time="2026-01-01", end_time="2026-02-01", dataframe_type="polars")`, and only the columns you need (a feature view `select()`, or `hops fg preview --columns` to check).
2. **Read one source FG at a time**, compute, `del` it before reading the next; do not keep copies of large frames.
3. **Insert in batches** with multi-part insert so the full output is never held in memory.
4. **Prefer Polars over Pandas** (columnar, zero-copy, lazy).
5. **Switch to PySpark** for very large data; it spills to disk (**hops-spark**).

## Selective statistics

If you want some statistics but not full computation:

```python
from hsfs.statistics_config import StatisticsConfig

fg = fs.get_or_create_feature_group(
    name="my_fg",
    version=1,
    statistics_config=StatisticsConfig(
        enabled=True,
        correlations=False,     # skip correlation matrix
        histograms=False,       # skip histograms
        exact_uniqueness=False, # skip uniqueness/entropy
        columns=["col1", "col2"],  # only compute for these columns
    ),
    ...
)
```

**Guidance to user:** When data volume is large (> a few GBs), inform the user that statistics are disabled by default for performance and ask if they want to enable them. Statistics are useful for data quality monitoring but expensive to compute at scale.

## Materialization

Schedule it at creation:
```python
fg = fs.get_or_create_feature_group(
    name="my_fg",
    version=1,
    offline_backfill_every_hr=4,  # materialize every 4 hours
    ...
)
```

You can also pass a cron expression string to `offline_backfill_every_hr` for more control.

Check the schedule:
```python
schedule = fg.offline_backfill_every_hr  # returns cron expression or int
job = fg.materialization_job
print(job.job_schedule)  # full schedule details
```

Run it by hand:

- `fg.materialization_job.run(await_termination=True)`: blocks until Spark job completes. Use when downstream steps need the offline data.
- `fg.materialization_job.run(await_termination=False)`: fires and forgets. Use when you just need the job scheduled.
