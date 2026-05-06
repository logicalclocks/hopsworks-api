---
name: hops
description: Use when working with Hopsworks — feature groups, feature views, training datasets, storage connectors, models, deployments, projects, jobs, and datasets. Auto-invoke when the user discusses feature engineering, feature store operations, ML pipelines, model serving, external data sources, Superset, or needs to interact with Hopsworks.
allowed-tools: Bash(hops *)
---

# Hopsworks CLI

You have access to the `hops` CLI to interact with the user's Hopsworks cluster.
Run read-only commands directly and confirm before destructive actions.

## Current Context

Project:
!`hops project info --json 2>/dev/null`

Feature Groups:
!`hops fg list --json 2>/dev/null`

Feature Views:
!`hops fv list --json 2>/dev/null`

Jobs:
!`hops job list --json 2>/dev/null`

## Authentication

```bash
hops setup                 # Browser token flow (first run on a new machine)
hops login --api-key ...   # Scripted fallback (CI, containers)
hops project use <name>    # Switch active project
```

Inside a Hopsworks terminal pod, authentication is automatic via JWT.

## Projects

```bash
hops project list                         # List projects
hops project use <name>                   # Switch active project
hops project info                         # Current project details
```

## Feature Groups

```bash
hops fg list                              # List all feature groups
hops fg info <name> [--version N]         # Show details + schema
hops fg preview <name> [--n 10]           # Preview data rows
hops fg features <name>                   # List features with types
hops fg stats <name> --compute            # Trigger statistics computation
hops fg search <name> --vector "0.1,..."  # KNN similarity search
hops fg keywords <name>                   # List tags (aka keywords)
hops fg add-keyword <name> <kw>           # Attach a keyword
hops fg remove-keyword <name> <kw>        # Remove a keyword
hops fg delete <name> --version N --yes   # Delete
```

### Create

```bash
hops fg create <name> --primary-key <cols> [flags]
```

Flags:
- `--primary-key <cols>` — comma-separated primary-key columns (required)
- `--features "name:type,..."` — schema (bigint, double, boolean, timestamp, string, `array<float>`)
- `--partition-key <cols>` — comma-separated partition columns
- `--online` — enable online storage (Kafka + RonDB + Spark materialization)
- `--event-time <col>` — event-time column for time-travel queries
- `--embedding "col:dim[:metric]"` — vector column, auto-enables online (metrics: cosine, l2, dot_product)
- `--description <text>` — free-form description
- `--version <n>` — explicit version (backend auto-assigns when omitted)

### External Feature Groups

```bash
hops fg create-external <name> \
  --connector <connector-name> \
  --query "SELECT ... FROM ..." \
  --primary-key <cols> \
  [--event-time <col>] [--description <text>]
```

### Insert

```bash
hops fg insert <name> --file data.csv     # Load CSV / JSON
hops fg insert <name> --generate 100      # Synthetic rows from the schema
cat data.json | hops fg insert <name>     # Stdin JSON
```

`--online` skips offline materialization (writes only to Kafka/RonDB).

### Derive

```bash
hops fg derive enriched \
  --base transactions \
  --join "products LEFT product_id=id p_" \
  --join "customers LEFT customer_id" \
  --primary-key order_id
```

Join spec: `"<fg>[:<ver>] <INNER|LEFT|RIGHT|FULL> <on>[=<right_on>] [prefix]"`.

## Feature Views

```bash
hops fv list
hops fv info <name> [--version N]
hops fv create <name> --feature-group <fg> [--join <spec>] [--transform <fn:col>]
hops fv get <name> --entry "id=42,region=eu"       # Online vector lookup
hops fv read <name> [--n 100] [--output data.parquet]
hops fv delete <name> --version N --yes
```

## Training Datasets

```bash
hops td list <fv> <fv-version>
hops td compute <fv> <fv-version>                   # Materialize
hops td compute <fv> <fv-version> --split "train:0.8,test:0.2"
hops td read <fv> <fv-version> --td-version N [--split train] [--output train.parquet]
hops td delete <fv> <fv-version> [--td-version N] --yes
```

## Transformations

```bash
hops transformation list
hops transformation create --file scaler.py
hops transformation create --code '@udf(float)
def double_it(value):
    return value * 2'
```

## Models

```bash
hops model list
hops model info <name> [--version N]
hops model register <name> <path> [flags]
hops model download <name> [--version N] [--output dir]
hops model delete <name> --version N --yes
```

Register flags:
- `--framework <sklearn|tensorflow|torch|python|llm>` (default: python)
- `--metrics "accuracy=0.95,auc=0.91"`
- `--feature-view <name>` + `--td-version <n>` — provenance + auto schema
- `--input-example <file>` — JSON file with a sample input
- `--description <text>`

## Deployments

```bash
hops deployment list
hops deployment info <name>
hops deployment create <model> [--version N] [--script predict.py]
hops deployment start <name> [--wait 600]
hops deployment stop <name>
hops deployment predict <name> --data '{"instances": [[1,2,3]]}'
hops deployment logs <name> [--component predictor] [--tail 50]
hops deployment delete <name> --yes
```

**Tips**
- sklearn serving image uses pinned versions — train with `scikit-learn==1.3.2` to match.
- Deployment names must be alphanumeric only.
- Artifact layout at inference time: predictor script at `/mnt/artifacts/`, model at `/mnt/models/`.

## Jobs

```bash
hops job list
hops job info <name>
hops job create <name> --type python --app-path Resources/jobs/x.py
hops job run <name> [--wait] [--args "..."]
hops job stop <name>
hops job logs <name> [--execution ID]
hops job history <name>
hops job schedule <name> "0 0 * * * ?"              # Quartz 6-field cron
hops job schedule-info <name>
hops job unschedule <name>
hops job delete <name> --yes
```

## Storage Connectors

```bash
hops datasource list
hops datasource info <name>
hops datasource databases <name>
hops datasource tables <name> --database X
hops datasource preview <name>
hops datasource delete <name> --yes
```

### Create

```bash
hops datasource create jdbc <name> --url "jdbc:..." --user u --password p
hops datasource create s3 <name> --bucket b --access-key ... --secret-key ... [--region eu-west-1]
hops datasource create snowflake <name> --url ... --user u --password p \
  --database D --schema S --warehouse W [--role R]
hops datasource create bigquery <name> --project-id gcp-proj --dataset D --key-path /Resources/key.json
```

## Superset

Wraps `project.get_superset_api()`. Use this for dashboards.

```bash
hops superset dataset list
hops superset dataset create --database-id N --table-name T [--schema S] [--sql "SELECT ..."]

hops superset chart list
hops superset chart create --name X --viz-type bar --datasource-id N --params '{"metrics":["count"]}'

hops superset dashboard list
hops superset dashboard create "My Dashboard" [--published]
```

## File system (HopsFS)

```bash
hops files list [path]
hops files mkdir /Projects/<proj>/newdir
hops files upload ./file.txt /Projects/<proj>/Resources/
hops files download /Projects/<proj>/Resources/out.log --output ./
hops files remove /Projects/<proj>/stale --yes

# Share a dataset with another project (Data Owner role required in source project)
hops files share Resources/my_dir --target other_project [--permission READ_ONLY|EDITABLE|EDITABLE_BY_OWNERS]
hops files unshare Resources/my_dir --target other_project
```

## Context and LLM Integration

```bash
hops context              # Markdown summary for LLM ingestion
hops context --json       # Same content as JSON
```

## Global Flags

```
--host <url>              # Override Hopsworks host
--api-key <key>           # Override API key
--project <name>          # Override project
--json                    # Machine-readable output
```

## Working with Hopsworks Efficiently

1. Start with `hops project list` then `hops project use <name>`.
2. Use `hops fg list` and `hops fv list` to discover resources.
3. Use `hops fg info <name>` before writing code that depends on a schema.
4. Use `hops context` for a full snapshot when you are about to plan work.
5. Use `--json` whenever parsing output programmatically.
6. Feature group and feature view names are case-sensitive.
