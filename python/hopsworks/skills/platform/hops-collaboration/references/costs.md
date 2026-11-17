# Compute cost per project, namespace or department

Each project runs in its own Kubernetes namespace; OpenCost prices each namespace's CPU, GPU, RAM, storage and network, and Hopsworks proxies it: the cluster-wide views to `HOPS_ADMIN` accounts, one project's cost to that project's data owners.

```python
mine = hopsworks.get_costs_api().get_project_costs("month")                 # the logged-in project; project="name" for another one where you are a data owner
print(mine.total_cost, mine.efficiency, mine.currency)                         # only the project's row: no idle, no share, no other projects
```

```bash
hops project costs --window month         # active project; --project NAME for another; --json for the raw report
```

The rest of this page is admin-only, and an API key needs the `ADMIN` scope; keys created by `hops setup` never have it.

```python
costs = hopsworks.get_costs_api().get_namespace_costs("month")  # hour | day | week (default) | month | year
print(costs.total_cost, costs.idle_cost, costs.efficiency, costs.currency)
print(costs.gpu_workload_cost, costs.cpu_workload_cost)          # namespace cost split by GPU vs CPU workloads
cov = costs.coverage                                               # observed_buckets, total_buckets, first_observed
df = costs.to_pandas()                                             # one row per namespace, most expensive first
for bucket in costs.buckets:                                       # per-slice rows for a chart
    if not bucket.observed:
        continue                                                   # no data: unknown cost, never plot it as 0
    print(bucket.start, bucket.partial, [(r.namespace, r.total_cost) for r in bucket.items])

dept = hopsworks.get_costs_api().get_department_costs("month")   # same window, totals and buckets, rows grouped by department
for row in dept.items:
    print(row.kind, row.department_name, row.namespace_count, row.total_cost)
```

```bash
hops admin costs --window month          # table: namespace, department, cpu, gpu, ram, storage, network, other, total, gpu workloads, cpu workloads, share, efficiency; plus a coverage line when buckets lack data
hops admin costs --window day --json
hops admin costs --by department         # table: department, namespaces, then the same cost columns
```

Key facts:
- Rows with `kind` `IDLE` (`__idle__`, node capacity nobody requested) and `UNMOUNTED` (`__unmounted__`) are not projects; they have no efficiency.
- Hour and day are live from OpenCost; week, month and year combine the daily history Hopsworks records with live data for the days not recorded yet, so a stored day is not recomputed and only days since recording started (or still in OpenCost's retention) are covered.
- `coverage.observed_buckets` of `coverage.total_buckets` have data, starting at `coverage.first_observed`; totals and rows cover only those buckets.
  A bucket with `observed=False` is no data (OpenCost not running yet, past retention, scrape gap), not zero cost; say "no data" when reporting it.
  `partial=True` marks the current bucket, whose cost is still growing.
- Each row's cost columns sum to `total_cost`; `share` is the row's fraction of `total_cost` (`None` when the total is not positive).
- `gpu_workload_cost` + `cpu_workload_cost` is a second split of a `NAMESPACE` row's `total_cost`, by workload: a Job, Deployment or other controller counts its whole cost (in each hour, or 10 minutes in the hour window, in which it requested GPUs) as GPU workload, CPU, RAM and storage included; `gpu_cost` is only the GPUs' own cost.
  Both are 0 on `IDLE` / `UNMOUNTED` rows.
- `NAMESPACE` rows carry `department_id` / `department_name`, `None` when the namespace is no project's or is shared by projects of different departments.
- The department view has one `DEPARTMENT` row per department (`namespace` is `None`, `namespace_count` namespaces summed in) and one `UNASSIGNED` row ("Not a project" in the CLI) for namespaces with no single department, such as Hopsworks' own, `kube-system` or deleted projects; `IDLE` / `UNMOUNTED` are as in the namespace view.
  Projects count in their current department, so moving a project moves its whole cost history; projects created without a department are in the default department.
  Its totals equal the namespace view's for the same window.
- Efficiency is used over requested core-hours or byte-hours; `cpu_no_requests` / `ram_no_requests` mean the namespace used the resource without requesting any, so its efficiency is meaningless; the CLI shows `No requests` when both are set.
- Cost tracking switched off on the cluster (`enable_opencost` false, the default) surfaces as `RestAPIError` with HTTP 404 and error code 100083.
- OpenCost not installed or unreachable surfaces as `RestAPIError` with HTTP 503 and error code 100081; for admins the cause is in `exc.user_message`.
- A project in a namespace shared with other projects gets `RestAPIError` HTTP 409, error code 100084, from `get_project_costs`: its cost cannot be split from theirs.
- A key without the `ADMIN` scope gets HTTP 403, error code 320004, from the admin views.
