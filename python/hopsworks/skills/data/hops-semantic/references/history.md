# Query history

Every query run over REST, Arrow Flight SQL or the BI tables is recorded, failed and cancelled ones included; members see their own, Data Owners everyone's.
Kept 30 days; the request, SQL and error go after 7 (`semantic_query_strip_details_after_days`).
Best effort, not an audit log: a record can be lost (full queue, expired credential). Metadata listings and `sql`/`--explain` are not recorded.
Use it to find why a dashboard or BI tool query failed (`--status FAILED`, the `error` column) or which surface and cache outcome a query had.

```bash
hops semantic history sales --status FAILED --limit 20            # newest first: surface, duration, rows, cache, error
hops semantic history sales --user meb10000 --json                # a member's queries; Data Owners only
```

```python
df = sales.query_history(limit=100, status="FAILED")                # pandas, one row per execution attempt
```
