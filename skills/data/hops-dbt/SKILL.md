---
name: hops-dbt
description: Use when writing or running dbt (dbt-core, dbt-trino, dbtRunner) transformations on Hopsworks. SQL models over feature-group tables in Trino, data tests as validation, results ingested into a feature group. Input a dbt project plus a Python runner; output a feature group, per-invocation dbt artifacts, and an interactive execution graph shown in the Jobs UI.
---

# dbt on Hopsworks (Trino)

Write dbt SQL models over offline feature groups, run them through the project's Trino, validate with dbt data tests, and land the result in a feature group. The runner is a plain Python program invoking dbt in-process, so the same script runs from a terminal and as a scheduled Hopsworks job.

## Contract
- **Input:** a dbt project (models over `<fg>_<version>` tables) plus a Python runner script.
- **Output:** transformed rows in a feature group, dbt test results, and per-invocation artifacts including `run_graph.json` and `execution_graph.html` under `Resources/dbt_runs/<job name>/<execution id>/`.
- **Pre-condition:** the source feature groups exist offline; the job runs in the `dbt-pipeline` environment or one cloned from it.

## Smoke-test (cheap pre/post-flight)
```bash
hops trino tables delta.<project>_featurestore     # sources visible to dbt?
hops job info <job-name>                           # after: execution finished?
```

## Environment

The stock `dbt-pipeline` environment ships `dbt-core`, `dbt-trino` (cluster runs), `dbt-duckdb` (local model development), and the `dbt_run_graph` module (execution graph, below). No environment build is needed; clone it via [hops-environments](../../platform/hops-environments/SKILL.md) only to add libraries.

## The dbt project

Wire `profiles.yml` to the project's Trino through env vars, never hardcoded credentials:

```yaml
my_project:
  target: hopsworks
  outputs:
    hopsworks:
      type: trino
      method: ldap
      host: "{{ env_var('DBT_TRINO_HOST') }}"
      port: "{{ env_var('DBT_TRINO_PORT') | int }}"
      user: "{{ env_var('DBT_TRINO_USER') }}"
      password: "{{ env_var('DBT_TRINO_PASSWORD') }}"
      database: delta
      schema: "{{ env_var('DBT_TRINO_SCHEMA') }}"
      cert: "{{ env_var('DBT_TRINO_CA') }}"
```

The runner fills those from the logged-in project:

```python
trino = project.get_trino_api()
user, password = trino.get_basic_auth()
os.environ.update({
    "DBT_TRINO_HOST": trino.get_host(),
    "DBT_TRINO_PORT": str(trino.get_port()),
    "DBT_TRINO_USER": user,
    "DBT_TRINO_PASSWORD": password,
    "DBT_TRINO_SCHEMA": f"{project.name}_featurestore",
    "DBT_TRINO_CA": str(trino._get_ca_chain_path(True)),
})
```

Rules that make dbt work against Hopsworks Trino:

- **Sources are feature-group tables:** `delta.<project>_featurestore.<fg name>_<version>` (see [hops-trino-sql](../hops-trino-sql/SKILL.md) for naming and partition pruning).
- **Materialize models as `ephemeral`.** The offline store is Hopsworks-managed: tables are created and written through the feature-store API so schema, statistics and lineage stay consistent. dbt compiles the SQL; the runner executes it and upserts the rows into a feature group.
- **Override relation listing.** dbt-trino's default introspection reads `system.metadata`, which Hopsworks Trino restricts. Add this macro to the project:

```sql
{% macro trino__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select table_catalog as database, table_name as name, table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else lower(table_type) end as table_type
    from {{ relation.information_schema() }}.tables
    where table_schema = '{{ relation.schema | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
```

## Running dbt from Python

Invoke dbt in-process with `dbtRunner`, one target directory per invocation. dbt overwrites `run_results.json` on every command, so a shared target directory loses run history; the per-execution directory is also where the Jobs UI looks for the execution graph.

```python
from dbt.cli.main import dbtRunner

def target_dir():
    job = os.environ.get("HOPSWORKS_JOB_NAME")
    execution = os.environ.get("HOPSWORKS_JOB_EXECUTION_ID")
    if job and execution and Path("/hopsfs").is_dir():
        return Path("/hopsfs/Resources/dbt_runs") / job / execution
    return Path("artifacts") / f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

run_dir = target_dir()
common = ["--project-dir", str(proj), "--profiles-dir", str(proj),
          "--target-path", str(run_dir), "--log-path", str(run_dir / "logs")]

runner = dbtRunner()
runner.invoke(["compile", *common, "--select", MODEL])   # writes compiled model SQL
build = runner.invoke(["build", *common, "--select", MODEL])  # runs the data tests
```

`build` (not `run`) includes validation: on an ephemeral model it executes the model's data tests, and `run_results.json` records status, timing and failing-row counts per test. Gate the ingestion on `build.success`. Then read the compiled model SQL from `<run_dir>/compiled/`, execute it over a Trino cursor, and upsert the rows into a feature group with `fg.multi_part_insert()` (see [hops-fg](../../ml/hops-fg/SKILL.md)).

## The execution graph

After the run, merge the artifacts into an interactive graph. The module is preinstalled in `dbt-pipeline`:

```python
import dbt_run_graph

ingest = {
    "id": "external.hopsworks.my_fg_1",
    "label": "feature group my_fg v1",
    "status": "success",                 # or "error"
    "execution_time": elapsed_seconds,
    "depends_on": [f"model.<dbt project>.{MODEL}"],
    "detail": {"rows_affected": row_count, "message": "..."},
}
dbt_run_graph.write_execution_graph(run_dir, external_nodes=[ingest],
                                    title=f"{project.name} · {MODEL}")
```

This writes into the target directory:

- `run_graph.json`: `manifest.json` and `run_results.json` joined on `unique_id` (DAG, per-node status, timing, failing rows, compiled SQL), plus non-dbt steps such as the ingestion as external nodes.
- `execution_graph.html`: a self-contained cytoscape.js page (dagre layout, nodes colored by status, tests attached to their models, side panel with compiled SQL, timing and errors). It inlines its libraries, so it opens from the dataset browser and on air-gapped clusters.

The Jobs UI shows an **Execution graph** button beside Logs on every execution of a Python job in a `dbt-pipeline`(-derived) environment, and it opens this page. Write the graph even when `build` fails, then raise: a failed run with a graph is far easier to debug.

## Deploying as a job

```bash
hops job deploy dbt-batch-features run_dbt_features.py --env dbt-pipeline --cron @daily
```

On older images whose `dbt-pipeline` predates the bundled module, stage `dbt_run_graph.py` and its `vendor/*.js` next to the script via the job config's `files` field; a co-located copy takes import precedence and needs no code change.

## Related skills

- [hops-trino-sql](../hops-trino-sql/SKILL.md): table naming, partition pruning, interactive queries.
- [hops-fg](../../ml/hops-fg/SKILL.md): the feature group the results land in.
- [hops-job](../../platform/hops-job/SKILL.md): job creation, scheduling, Airflow chaining.
- [hops-environments](../../platform/hops-environments/SKILL.md): cloning `dbt-pipeline` to add libraries.
