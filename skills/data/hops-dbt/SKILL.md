---
name: hops-dbt
description: Use whenever dbt is involved on Hopsworks (dbt-core, dbt-trino, dbt-duckdb, dbtRunner, `dbt build/run/test`, a dbt project with models/*.sql and profiles.yml). SQL models over feature-group tables, data tests as validation, results landed in a feature group, and the per-execution execution graph the Jobs UI shows. Auto-invoke when the user mentions dbt, asks to run dbt models as a Hopsworks job, or reports a missing execution graph. Input a dbt project plus a Python runner; output a feature group, per-execution dbt artifacts, and an interactive execution graph.
---

# dbt on Hopsworks

Write dbt SQL models over offline feature groups, run them from a Python runner, validate with dbt data tests, land the result in a feature group, and leave the run's artifacts where the Jobs UI reads its **Execution graph** from. The runner is a plain Python program, so the same script runs from a terminal and as a scheduled Hopsworks job.

## Contract
- **Input:** a dbt project (models over `<fg>_<version>` tables, or over data the runner exports) plus a Python runner script.
- **Output:** transformed rows in a feature group, dbt test results, and the per-execution artifacts `manifest.json`, `run_results.json`, `run_graph.json` and `execution_graph.html` under `Resources/dbt_runs/<job name>/<execution id>/`.
- **Pre-condition:** the source feature groups exist offline; the job runs in the `dbt-pipeline` environment or one cloned from it.

## The artifact rule (read before writing the runner)

The Jobs UI shows an **Execution graph** button on every execution of a Python
job in a `dbt-pipeline`(-derived) environment, and it reads
`Resources/dbt_runs/<job name>/<execution id>/run_graph.json`. A runner that does
not put the file there produces a FINISHED job whose button says
*"No execution graph was produced by this execution."*, which reads like a
platform failure. Three rules, none optional:

1. **Every dbt invocation runs with `--target-path <run_dir>` and `--log-path <run_dir>/logs`**, where `run_dir` is `/hopsfs/Resources/dbt_runs/$HOPSWORKS_JOB_NAME/$HOPSWORKS_JOB_EXECUTION_ID` (both env vars are set in job pods, and HopsFS is mounted at `/hopsfs`). Not `dbt_project.yml`'s `target-path`, not the project directory, not a `tempfile` scratch dir.
2. **The runner ends by calling `dbt_run_graph.write_execution_graph(run_dir, ...)`**, on failure as well as success. The module is preinstalled in `dbt-pipeline`.
3. **Never delete the run directory.** Clean up the scratch copy of the dbt project, not the artifacts.

The target database is irrelevant to the rule: a Trino target and a DuckDB target
over exported Parquet produce the same artifacts.

## Smoke-test (cheap pre/post-flight)
```bash
hops trino tables delta.<project>_featurestore                 # before: sources visible to dbt?
hops job info <job-name>                                       # after: execution FINISHED?
hops files list Resources/dbt_runs/<job-name>/<execution id>   # after: run_graph.json + execution_graph.html present
```

## Environment

The stock `dbt-pipeline` environment ships `dbt-core`, `dbt-trino` (cluster runs), `dbt-duckdb` (local model development, or a local target over exported data), and the `dbt_run_graph` module. No environment build is needed; clone it via [hops-environments](../../platform/hops-environments/SKILL.md) only to add libraries.

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

**DuckDB variant.** When the models should run on a local engine, export the source feature group to Parquet (`fg.read().to_parquet(...)`) in a scratch dir, point a `duckdb` profile at a `.duckdb` file there, and read the model's table back with `duckdb.connect(...)`. Run dbt in a **subprocess** in that case (`python -m dbt.cli.main build ...`): the in-process runner keeps the DuckDB file open and blocks the read-back. The artifact rule is unchanged.

## Runner skeleton

Copy this shape; the marked lines are the only project-specific parts. It runs dbt in-process with `dbtRunner`, one target directory per execution (dbt overwrites `run_results.json` on every command, so a shared directory loses run history), gates the ingestion on the build, and writes the graph on every path.

```python
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import hopsworks
from dbt.cli.main import dbtRunner

DBT_PROJECT = Path(__file__).parent / "my_dbt_project"    # project-specific
DBT_PROJECT_NAME = "my_project"                             # `name:` in dbt_project.yml
MODEL = "customer_margin_monthly"                           # the model to land
TARGET_FG = ("customer_margin_monthly", 1)                  # sink feature group


def run_dir() -> Path:
    # The Jobs UI reads Resources/dbt_runs/<job>/<execution id>/run_graph.json;
    # the local fallback keeps the script runnable from a terminal.
    job = os.environ.get("HOPSWORKS_JOB_NAME")
    execution = os.environ.get("HOPSWORKS_JOB_EXECUTION_ID")
    if job and execution and Path("/hopsfs").is_dir():
        return Path("/hopsfs/Resources/dbt_runs") / job / execution
    return Path("artifacts") / f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"


def write_graph(target_dir: Path, ingest: dict) -> None:
    import dbt_run_graph  # preinstalled in dbt-pipeline

    out = dbt_run_graph.write_execution_graph(
        target_dir, external_nodes=[ingest], title=f"{DBT_PROJECT_NAME} · {MODEL}"
    )
    print(f"execution graph written to {out}")


def main() -> int:
    project = hopsworks.login()
    fs = project.get_feature_store()
    # ... export the DBT_TRINO_* env vars (Trino) or the source Parquet (DuckDB) here

    target_dir = run_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    common = ["--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT),
              "--target-path", str(target_dir), "--log-path", str(target_dir / "logs")]
    ingest = {
        "id": f"external.hopsworks.{TARGET_FG[0]}_{TARGET_FG[1]}",
        "label": f"feature group {TARGET_FG[0]} v{TARGET_FG[1]}",
        "status": "skipped",
        "execution_time": None,
        "depends_on": [f"model.{DBT_PROJECT_NAME}.{MODEL}"],
        "detail": {"message": "not run: dbt build failed"},
    }

    runner = dbtRunner()
    runner.invoke(["compile", *common, "--select", MODEL])        # writes compiled SQL
    build = runner.invoke(["build", *common, "--select", MODEL])  # models + data tests
    if not build.success:
        write_graph(target_dir, ingest)      # a failed run with a graph is debuggable
        return 1

    started = datetime.now(timezone.utc)
    try:
        rows = run_compiled_sql_and_upsert(fs, target_dir, MODEL, TARGET_FG)   # project-specific
    except Exception as e:
        ingest.update(status="error", detail={"message": str(e)},
                      execution_time=(datetime.now(timezone.utc) - started).total_seconds())
        write_graph(target_dir, ingest)
        raise
    ingest.update(status="success", detail={"rows_affected": rows},
                  execution_time=(datetime.now(timezone.utc) - started).total_seconds())
    write_graph(target_dir, ingest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

`build` (not `run`) includes validation: on an ephemeral model it executes the model's data tests, and `run_results.json` records status, timing and failing-row counts per test. For the Trino path, `run_compiled_sql_and_upsert` reads the compiled model SQL from `<run_dir>/compiled/`, executes it over a Trino cursor, and upserts the rows into the feature group with `fg.multi_part_insert()` (see [hops-fg](../../ml/hops-fg/SKILL.md)). For the DuckDB path it reads the model's table from the `.duckdb` file and inserts it.

External node statuses the graph understands: `success`, `error`, `skipped`, `warn`, `pass`, `fail`.

## The execution graph

`write_execution_graph` writes into the target directory:

- `run_graph.json`: `manifest.json` and `run_results.json` joined on `unique_id` (DAG, per-node status, timing, failing rows, compiled SQL), plus non-dbt steps such as the ingestion as external nodes. This is what the Jobs UI renders.
- `execution_graph.html`: a self-contained cytoscape.js page (dagre layout, nodes colored by status, tests attached to their models, side panel with compiled SQL, timing and errors). It inlines its libraries, so it opens from the dataset browser and on air-gapped clusters.

## Deploying as a job

```bash
hops job deploy dbt-batch-features run_dbt_features.py --env dbt-pipeline --cron @daily
hops job run dbt-batch-features --wait                          # or wait for the schedule
hops files list Resources/dbt_runs/dbt-batch-features/<execution id>   # run_graph.json present?
```

`hops job deploy` uploads the script to `Resources/jobs/<job name>/`; a dbt project that lives next to the script in HopsFS (as in the skeleton's `Path(__file__).parent`) is uploaded separately with `hops files upload`, or fetched at run time with `project.get_dataset_api().download(...)`.

On older images whose `dbt-pipeline` predates the bundled module, stage the `dbt_run_graph/` package (its `__init__.py` and `vendor/*.js`) next to the script via the job config's `files` field; a co-located copy takes import precedence and needs no code change.

## Related skills

- [hops-trino-sql](../hops-trino-sql/SKILL.md): table naming, partition pruning, interactive queries.
- [hops-fg](../../ml/hops-fg/SKILL.md): the feature group the results land in.
- [hops-job](../../platform/hops-job/SKILL.md): job creation, scheduling, Airflow chaining.
- [hops-environments](../../platform/hops-environments/SKILL.md): cloning `dbt-pipeline` to add libraries.
