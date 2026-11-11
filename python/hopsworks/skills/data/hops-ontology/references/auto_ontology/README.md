# Building an ontology system

The build of the `ontology` factory, phase by phase, for Claude Code in a Hopsworks terminal, run as `/hops-factory-ontology <slug>` in the system's directory.
Each phase starts with `status: running` and `started` in its block of `system.yaml` and ends with `status: met` and `finished`, written as the factory command says (never in place: `hops factory system write-doc`).
Names and shapes are fixed by the HWORKS-3744 contract; nothing here may rename them.

## Before the first phase

```bash
SLUG=<slug>                                  # the system, also the app's name
SLUG_=${SLUG//-/_}                           # feature groups use underscores
REF=/opt/hopsworks-api/python/hopsworks/skills/data/hops-ontology/references/auto_ontology
SYSTEM_DIR=$(python -c "import os, pathlib; h = pathlib.Path(os.environ['HOPSFS_USER_HOME_DIR']); print(pathlib.Path.cwd().resolve().relative_to(h.parent.parent.resolve()))")
echo "$SYSTEM_DIR"                           # project-relative, such as Users/meb10000/sales-ontology
AO_COMMIT=$(grep -o '@[0-9a-f]\{40\}' $REF/requirements.txt | tr -d @)
ENV=ontology-${AO_COMMIT:0:8}                # the environment the app and the jobs run in
echo "$ENV"
```

Outside a terminal, `REF` is `skills/data/hops-ontology/references/auto_ontology` under the installed `hopsworks` package.

The form's answers are in `requirements`, each under its field id:

| Key | Values |
| --- | --- |
| `requirements.sources` | a list of `{name, schemas, table_regex}`; `schemas` is a comma-separated allowlist or empty |
| `requirements.llm_mode`, `requirements.embedding_mode` | `local` or `external` |
| `requirements.embedding_dim` | the string `"2048"` or `"1024"` |
| `requirements.ingest_cadence`, `requirements.compile_cadence` | `daily`, `weekly` or `on_demand` |
| `requirements.pii_detection` | true or false |
| `requirements.example` | `wwi` for the sales example, else absent |
| `requirements.description` | who asks questions, and about what |

An example system never asks: a gap goes under `unavailable` and is named at the end.

## reqs

1. **The sales example.** With `requirements.example: wwi`, run the example setup as a Data owner (a Data scientist cannot create tables):

   ```bash
   python $REF/examples/wwi/setup.py
   ```

   It creates the `sales_*` tables in the project's online database, fills them from a fixed seed, creates the MySQL data source `sales_mysql` with SHARED credentials, and prints the one `sources` entry to use, such as `{"name": "sales_mysql", "schemas": ["demo"], "table_regex": "^sales_"}`.
   The schema is the project's online database: a member cannot create a database named `sales`, so the tables carry the `sales_` prefix instead.
   That entry replaces `requirements.sources`.
2. **Check every source.** Each must exist, be a supported type, and have SHARED credentials: a PROVIDED source answers with one member's credentials while the app and the jobs answer every member.

   ```python
   import hopsworks

   SUPPORTED = {("SQL", "MYSQL"), ("SQL", "POSTGRESQL"), ("SQL", "CLICKHOUSE"), ("SNOWFLAKE", None), ("UNITY_CATALOG", None)}
   fs = hopsworks.login().get_feature_store()
   for name in ["<each requirements.sources[].name>"]:
       sc = fs.get_data_source(name).storage_connector          # None: the project has no data source by that name
       if sc is None:
           print(name, "MISSING")
           continue
       kind = (sc.type, getattr(sc, "database_type", None) if sc.type == "SQL" else None)
       mode = getattr(sc, "credentials_mode", "SHARED")
       print(name, kind, mode, "ok" if kind in SUPPORTED and mode == "SHARED" else "REFUSED")
   ```

   Supported are MySQL, PostgreSQL and ClickHouse SQL data sources, Snowflake, and Databricks (Unity Catalog).
   A missing or refused source stops the build: set `reqs` `unmet` with the reason, naming the source and, when it exists, its type and its credentials mode.
3. **External modes.** For `llm_mode: external`, `LLM_URL` and `LLM_MODEL` must be account variables; for `embedding_mode: external`, `EMBEDDING_URL` and `EMBEDDING_MODEL`. Check them by name only, never by value:
   `python -c "import hopsworks; hopsworks.login(); print(sorted(v.name for v in hopsworks.get_env_vars_api().get_env_vars(include_value=False)))"`.
   An external embedding model must return vectors of `requirements.embedding_dim` dimensions.
4. **ontology.yaml.** Write `ontology.yaml` in this directory from `$REF/ontology.yaml.template`, the only file written from the answers:
   - `name` and `app` are the slug; the feature groups are `<slug_>_data_objects` and `<slug_>_semantic`, version 1;
   - `embedding.mode` and `llm.mode` are the two modes; in external mode `embedding.model` and `llm.model` are null, and the runtime reads `EMBEDDING_MODEL` and `LLM_MODEL`;
   - `embedding.dimension` is `requirements.embedding_dim` as a number, and `embedding.request` stays `v1-prefix`;
   - `sources` has one entry per source, `schemas` split on commas (null when empty) and `table_regex` null when empty;
   - `pii_detection` is `requirements.pii_detection`.

Record `reqs: {status: met, sources: [{name, type, credentials: SHARED}]}`.

## env

The app and the jobs run in `$ENV`, a clone of `python-feature-pipeline` with Auto-Ontology installed from the commit of the Hopsworks fork that `$REF/requirements.txt` pins.
The fork commits the built web UI, so the install needs no Node.
The name carries the commit: a newer pin makes a new environment, and systems built on an older one keep theirs.
It is shared by the project's ontology systems, so reuse it when another system already made it.
Cloning and installing the package's dependencies takes several minutes: say so before it starts.

```bash
cp $REF/requirements.txt .
```

```python
import hopsworks

ENV, SYSTEM_DIR = "<ENV>", "<SYSTEM_DIR>"
env_api = hopsworks.login().get_environment_api()
if env_api.get_environment(ENV) is None:
    env = env_api.create_environment(
        ENV,
        description="Auto-Ontology for the project's ontology systems",
        base_environment_name="python-feature-pipeline",
        await_creation=True,
    )
    env.install_requirements(f"{SYSTEM_DIR}/requirements.txt", await_installation=True)
print(ENV, "ready")
```

`install_requirements` returning without an error is the success signal.
A failed install leaves the clone behind without the package: read the error, delete the clone with `env_api.get_environment(ENV).delete()`, fix the cause and run the block again.

Record `env: {status: met, name: <ENV>, auto_ontology_commit: <AO_COMMIT>}`.

## models

With both modes external there is nothing to do: set the phase `skipped`.
Otherwise each local model is imported and deployed once per project and shared by every ontology system in it: reuse what another system already made.

| Role | Hugging Face id | Revision | Registry name | Deployment | vLLM config |
| --- | --- | --- | --- | --- | --- |
| embed (`embedding_mode: local`) | `nvidia/Nemotron-3.5-Embed-1B-BF16` | `e24ca1e346bcc53572bd7bdc02b3e8ebf66c197c` | `Nemotron_3_5_Embed_1B_BF16` | `nemotronembed` | `vllm/embed.yaml` |
| llm (`llm_mode: local`) | `RedHatAI/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-FP8` | `bff279eb543e15f7c0e3f922661e66a1b78ac2e0` | `NVIDIA_Nemotron_3_5_Lightning_30B_A3B_FP8` | `nemotronllm` | `vllm/llm.yaml` |

The LLM checkpoint is about 32 GB, so its import takes a while: say so before it starts.
The two deployments share one GPU through time-slicing (a cluster prerequisite: the NVIDIA device plugin with `replicas: 2`), each requesting `nvidia.com/gpu: 1` and running one replica; their `gpu-memory-utilization` caps (0.12 and 0.80) are what keep them apart.
The configs carry no `served-model-name`: Hopsworks passes the deployment name, which is the `model` of every request.

```python
import os

import hopsworks
from hsml.llm.predictor import Predictor as VllmPredictor
from hsml.resources import PredictorResources, Resources
from hsml.scaling_config import PredictorScalingConfig

SLUG = "<slug>"
REF = "/opt/hopsworks-api/python/hopsworks/skills/data/hops-ontology/references/auto_ontology"
CONFIGS = "Resources/ontology-vllm"   # shared by the project's ontology systems, outlives each of them
WANTED = [  # keep only the roles whose mode is local, embed first
    ("embed", "nvidia/Nemotron-3.5-Embed-1B-BF16", "e24ca1e346bcc53572bd7bdc02b3e8ebf66c197c",
     "Nemotron_3_5_Embed_1B_BF16", "nemotronembed", "embed.yaml", 2, 8192),
    ("llm", "RedHatAI/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-FP8", "bff279eb543e15f7c0e3f922661e66a1b78ac2e0",
     "NVIDIA_Nemotron_3_5_Lightning_30B_A3B_FP8", "nemotronllm", "llm.yaml", 4, 32768),
]

project = hopsworks.login()
mr, ms, datasets = project.get_model_registry(), project.get_model_serving(), project.get_dataset_api()
if not datasets.exists(CONFIGS):
    datasets.mkdir(CONFIGS)
for role, repo, revision, registry_name, name, config, cores, memory in WANTED:
    model = next((m for m in mr.get_models(registry_name) or [] if revision in (m.description or "")), None)
    if model is None:
        model = mr.hf_download(repo, hf_token=os.environ.get("HF_TOKEN") or None, revision=revision)
    datasets.upload(f"{REF}/vllm/{config}", CONFIGS, overwrite=True)
    deployment = ms.get_deployment(name)
    if deployment is None:
        # Built directly: hf_download registers the embedding model as TORCH, which deploy() would serve without vLLM.
        deployment = VllmPredictor(
            name=name,
            model_name=model.name,
            model_path=model.model_path,
            model_version=model.version,
            config_file=f"/Projects/{project.name}/{CONFIGS}/{config}",
            description=f"The {role} model of the project's ontology systems, created by {SLUG}",
            resources=PredictorResources(
                requests=Resources(cores=cores, memory=memory, gpus=1),
                limits=Resources(cores=cores, memory=memory, gpus=1),
            ),
            scaling_configuration=PredictorScalingConfig(min_instances=1, max_instances=1),
        ).deploy()
    if not deployment.is_running():
        deployment.start(await_running=1800)
    print(role, model.name, model.version, deployment.name, deployment.get_state().status)
```

The description names the slug on purpose: `hops factory system delete --assets` keeps a shared deployment while any other system names it, and deletes it with the last one only when its description names that system.
A deployment that does not reach Running: read `hops deployment logs <name>`; out of GPU memory means the other deployment holds more than its cap, and no GPU means time-slicing is missing on the node.

Check that each answers one request, with the deployment name as the model:

```python
client = ms.get_deployment("nemotronllm").get_openai_client()
print(client.chat.completions.create(
    model="nemotronllm", max_tokens=16, messages=[{"role": "user", "content": "Say OK."}],
    extra_body={"chat_template_kwargs": {"enable_thinking": False}},
).choices[0].message.content)
client = ms.get_deployment("nemotronembed").get_openai_client()
print(len(client.embeddings.create(model="nemotronembed", input=["query: How many orders?"]).data[0].embedding))  # 2048
```

Record, leaving out what an external mode does not use:

```yaml
models: {status: met, revisions: {embed: e24ca1e346bcc53572bd7bdc02b3e8ebf66c197c, llm: bff279eb543e15f7c0e3f922661e66a1b78ac2e0}, configs: Resources/ontology-vllm}
embedding_model: {name: Nemotron_3_5_Embed_1B_BF16, version: <v>}
model: {name: NVIDIA_Nemotron_3_5_Lightning_30B_A3B_FP8, version: <v>}
serving:
  deployments:
    - {deployment: nemotronllm,   role: llm,   model: NVIDIA_Nemotron_3_5_Lightning_30B_A3B_FP8, model_version: <v>, shared: true}
    - {deployment: nemotronembed, role: embed, model: Nemotron_3_5_Embed_1B_BF16, model_version: <v>, shared: true}
```

With `llm_mode: external` leave out `model` and the `nemotronllm` entry; with `embedding_mode: external` leave out `embedding_model` and the `nemotronembed` entry.

## app

Copy the launchers verbatim, then create and start the app.

```bash
cp $REF/app.py . && mkdir -p jobs && cp $REF/jobs/*.py jobs/
```

```python
import hopsworks

SLUG, SYSTEM_DIR, ENV = "<slug>", "<SYSTEM_DIR>", "<ENV>"
apps = hopsworks.login().get_app_api()
app = apps.get_app(SLUG) or apps.create_app(
    SLUG,
    app_path=f"{SYSTEM_DIR}/app.py",        # project-relative: the app runs in /hopsfs/<SYSTEM_DIR> and reads ./ontology.yaml
    environment=ENV,
    memory=4096,
    cores=2.0,
    app_kind="CUSTOM",
    entrypoint_command="python app.py",
    app_port=8080,
    readiness_probe_path="/health",
    db_access=True,
    idle_scale_to_zero=False,
    sign_identity=True,                     # the app reads each caller's name, and so their project role, from the proxy
    description=f"Ontology and text-to-SQL app of the ontology system {SLUG}",
)
app.run(await_serving=True)
app = apps.get_app(SLUG)
print(app.state, app.serving, app.table_prefix, app.table_count)
```

Serving means `/health` answered the readiness probe; a `table_count` above zero means the app created its tables on start.
Then check it through the app proxy: `hops ontology status <slug>` prints the facts and, before ingest and compile, exits 1 with those two as blockers, which is expected here; an HTTP error is not.
A failed start: `hops app logs <slug>`, fix, `hops app start <slug>`, at most five times.

Record `app: {status: met, name: <slug>, url: /hopsworks-api/pythonapp/<project>/<slug>/, table_prefix: <prefix>}`: the phase's block is the app's.

## ingest

Create the five jobs, each a PYTHON job in `$ENV` with 2 cores, 4096 MB and no GPU, whose default arguments name `ontology.yaml` by its project-relative path:

```python
import hopsworks

SLUG, SYSTEM_DIR, ENV = "<slug>", "<SYSTEM_DIR>", "<ENV>"
project = hopsworks.login()
api = project.get_job_api()
for name, launcher in [("ingest", "ingest"), ("compile", "compile"), ("embed", "embed"), ("reset", "reset"), ("import", "import_model")]:
    job_name = f"{SLUG}-{name}"
    if api.get_job(job_name) is None:
        config = api.get_configuration("PYTHON")
        config["appPath"] = f"/Projects/{project.name}/{SYSTEM_DIR}/jobs/{launcher}.py"
        config["environmentName"] = ENV
        config["defaultArgs"] = f"--config {SYSTEM_DIR}/ontology.yaml"
        config["resourceConfig"].update(cores=2.0, memory=4096, gpus=0)
        api.create_job(job_name, config)
    # create_job's response carries no creator; read the job back for it.
    print(job_name, api.get_job(job_name).creator["username"])
```

Schedule ingest and compile by their cadences, ingest an hour before compile:

| Cadence | `<slug>-ingest` | `<slug>-compile` |
| --- | --- | --- |
| `daily` | `0 0 2 * * ?` | `0 0 3 * * ?` |
| `weekly` | `0 0 2 ? * MON` | `0 0 3 ? * MON` |
| `on_demand` | no schedule | no schedule |

```bash
hops job schedule $SLUG-ingest "0 0 2 * * ?"
hops job schedule $SLUG-compile "0 0 3 * * ?"
hops job run $SLUG-ingest --wait
```

A scheduled run starts as the job's creator, so record each job's creator.
Check the result: the ingest run ends `SUCCEEDED` (else `hops job logs $SLUG-ingest --stdout`), `hops ontology status <slug>` lists the sources' databases with `catalog_present` true, and the data-objects feature group has a row for each table and column in scope:

```python
import hopsworks

fs = hopsworks.login().get_feature_store()
print(fs.get_feature_group("<slug_>_data_objects", 1).read(online=True).shape)
```

Record:

```yaml
features:
  pipelines:
    - {name: ingest,  job: {name: <slug>-ingest,  schedule: "0 0 2 * * ?", creator: <username>}}
    - {name: compile, job: {name: <slug>-compile, schedule: "0 0 3 * * ?", creator: <username>}}
    - {name: embed,   job: {name: <slug>-embed}}
    - {name: reset,   job: {name: <slug>-reset}}
    - {name: import,  job: {name: <slug>-import}}
  writes:
    - {feature_group: <slug_>_data_objects, version: 1}
    - {feature_group: <slug_>_semantic, version: 1}
ingest: {status: met, tables: <n>, data_object_rows: <n>}
```

`on_demand` records no `schedule`.

## compile

```bash
hops job run $SLUG-compile --wait
```

It sends many LLM calls; on one GPU, expect tens of minutes for a few dozen tables.
Then check the three exit conditions with the member's own database user, which reads the app's tables (`p` is the app's `table_prefix`):

```python
from urllib.parse import urlsplit

import hopsworks
import pymysql

SLUG, SLUG_ = "<slug>", "<slug_>"
project = hopsworks.login()
fs = project.get_feature_store()
p = project.get_app_api().get_app(SLUG).table_prefix
options = fs.get_online_storage_connector().spark_options()
url = urlsplit(options["url"].removeprefix("jdbc:"))
with pymysql.connect(host=url.hostname, port=url.port or 3306, user=options["user"], password=options["password"], database=url.path.lstrip("/")) as conn, conn.cursor() as cur:
    # 1. Every eligible table has a Term; a table whose columns are all foreign keys legitimately gets none.
    cur.execute(f"""SELECT t.name FROM {p}catalog_table t
        WHERE NOT EXISTS (SELECT 1 FROM {p}table__term tt WHERE tt.table_id = t.id)
          AND EXISTS (SELECT 1 FROM {p}catalog_column c WHERE c.table_id = t.id
                      AND NOT EXISTS (SELECT 1 FROM {p}column__foreign_key f WHERE f.source_column_id = c.id))""")
    print("tables without a Term:", [r[0] for r in cur.fetchall()])
    # 2 and 3. What each feature group holds, by the app's own record of it.
    cur.execute(f"SELECT collection, COUNT(*) FROM {p}vector_row GROUP BY collection")
    print("vector rows:", cur.fetchall())
print("semantic rows:", fs.get_feature_group(f"{SLUG_}_semantic", 1).read(online=True).shape[0])
```

- **Terms.** The first list is empty.
- **Vector work.** Run `hops job run $SLUG-embed --wait` once: it reconciles every ontology row whose text is not embedded, or embedded from older text, and fails when a row stays unreconciled.
- **Rows.** The semantic feature group's row count equals the `vector_row` count of its collection.

A condition that fails after one more compile and embed: set `compile` `unmet` with the numbers.
Record `compile: {status: met, terms: <n>, semantic_rows: <n>, tables_without_term: []}`.

## verify

Ask the questions: for `wwi` the five in `$REF/examples/wwi/questions.yaml`, otherwise three drawn from `requirements.description`.

```bash
time hops ontology ask $SLUG "How many orders were placed in March 2025?" --json
```

Each answer prints the answer text, its SQL and its rows (at most 100, with `truncated`).
For `wwi`, compare each with `answer` in `questions.yaml`; its `sql` is the reference query.
Then run `hops ontology status <slug>` (exits 0 when ready) and `hops factory system status <slug>`, fix what fails, and run them again.

Record each question, answer, SQL and time, and set `system.status: built`:

```yaml
verify:
  status: met
  questions:
    - {question: "How many orders were placed in March 2025?", answer: "14", sql: "SELECT ...", seconds: 21.4, matches: true}
  last_run: {when: <UTC>, problems: []}
```

Tell the user where the app is (`hops app url <slug>`, and the system's App view in the Factory), that `hops ontology ask` and the MCP tools ask it from a terminal, and that Observers and Feature store restricted members get the catalog read-only and no chat, since chat runs SQL on the data sources.
