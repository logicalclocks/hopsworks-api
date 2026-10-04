---
name: hops-partitioning
description: Use when deciding whether a feature group built from another table (a silver table from bronze, any derived table) should be partitioned, and by which time grain (hour, day or week). Reads the source table's Parquet files on the terminal's HopsFS mount to measure its volume and time span. Auto-invoke during the design of a silver table (/hops-silver) or when a user asks whether to partition a feature group.
---

# Partitioning a derived feature group by time

Partitioning pays when a table is large and its reads and writes target a time range: each incremental run of a silver job writes one window, and queries filter on time.
It costs when partitions are small: every partition holds its own files, so a small table split by the hour becomes thousands of tiny files.
Decide from the data, not by default.

## Contract

- **Input:** the source (bronze) feature group and its time column (the arrival or event time column the job windows on).
- **Output:** a decision, `none` or `partition` by `hour`, `day` or `week`, with the evidence, recorded in the table's design (`system.yaml` `partitioning`).
- **Pre-condition:** a Hopsworks terminal, where offline feature groups are mounted read-only at `/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>`.

## Measure the source

```bash
T=/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>
du -sh "$T"; ls "$T" | head                      # size, and existing partition directories (col=value)
python <skills>/data/hops-partitioning/scripts/partition_advisor.py "$T" --time-column <arrival_column>
```

The script reads only the Parquet footers: total bytes, rows, and the time column's minimum and maximum, from which it gets the bytes per hour, day and week.
For a Delta table, files removed by later commits stay on disk until the table is vacuumed, so the bytes are an upper bound; `_delta_log` holds the active set (hops-table-maintenance reads it).
A silver table holds about what its bronze sources hold after deduplication, so the source volume is the estimate; a silver table built from several sources sums them, and a lookup or entity table that is far smaller than its source is judged on its own expected rows.

## The rule

| Evidence | Decision |
| --- | --- |
| under 1 GiB and 10 million rows | `none`: one partition, files kept compact |
| bytes per hour at least 256 MiB | `partition` by `hour` |
| else bytes per day at least 256 MiB | `partition` by `day` |
| else bytes per week at least 256 MiB | `partition` by `week` |
| else | `none`: cluster on the time column instead (hops-delta) |

The finest grain whose partitions reach 256 MiB wins, as long as the table stays under 50,000 partitions.
Only time-partition a table whose reads and writes are by time; an entity table (customers, products) written as a whole is not partitioned by time, whatever its size, and is clustered on its key instead.

## Partitioning the feature group

Partition on a column derived from the time column, never on the raw timestamp, which would make one partition per distinct value:

| Grain | Column | Value |
| --- | --- | --- |
| hour | `<time>_hour` | `date_trunc('hour', <time>)` as a string `yyyy-MM-dd-HH` |
| day | `<time>_date` | `cast(<time> as date)` |
| week | `<time>_week` | `date_trunc('week', <time>)` as a date (the Monday) |

```python
fg = fs.get_or_create_feature_group(
    name="orders", version=1, primary_key=["order_id"], event_time="ordered_at",
    partition_key=["ordered_at_date"], time_travel_format="DELTA", online_enabled=False,
    description="...",
)
```

The job computes the partition column with the rest of the row, so a window writes into the partitions it covers.
Changing a table's partitioning is a new feature group version: record the decision before the first write.

## Next Steps

- File compaction and clustering of an existing table: **hops-table-maintenance**, **hops-delta**.
- Where a silver layer records the decision: **hops-medallion**.
