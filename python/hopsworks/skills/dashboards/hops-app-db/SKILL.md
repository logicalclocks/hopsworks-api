---
name: hops-app-db
description: Use when a Hopsworks Python app or agent deployment needs its own
  database (app state, sessions, agent memory) or wants to query the project's
  online feature store tables with SQL from inside the pod. Explains the
  `MYSQL_*` variables that `db_access` injects, how to resolve the password
  secret, which privileges the connection has, and the RonDB table rules.
  Auto-invoke when user asks how an app connects to a database, where to store
  app state, or how to read the online feature store with SQL. Examples in
  Python (PyMySQL / SQLAlchemy), plain SQL, and Node.js.
---

# App database access — the project's online feature store database

## Concept

Every project owns one MySQL-compatible database on **RonDB**, the online
feature store, named after the project (lowercase). It holds the online feature
group tables (`<fg_name>_<version>`) and it is also where an app keeps its own
tables: sessions, user preferences, agent memory, job results. There is no
separate "app database"; the app and the feature store share the project schema.

The database is **created on demand**. Historically that happened with the first
online feature group; now it also happens when a Python app with `db_access=True`
(the default) or an agent deployment starts. Both get the same five environment
variables, so code written for one works in the other.

## Key facts / rules

### What the pod gets

| Variable | Value |
|---|---|
| `MYSQL_HOST` | internal DNS name of the online feature store MySQL server |
| `MYSQL_PORT` | its port (normally `3306`) |
| `MYSQL_DB` | the project database, `<project_name>` lowercased |
| `MYSQL_USER` | the MySQL user of the person who **started** the app, `<project>_<username>` |
| `MYSQL_PASSWORD_SECRET_NAME` | name of the **Hopsworks secret** holding that user's password (same string as `MYSQL_USER`) |

- The password is never put in the environment. Read the secret named by
  `MYSQL_PASSWORD_SECRET_NAME` through the SDK or the REST API; it is a private
  secret of the starting user, and the pod authenticates as that user with the
  job JWT in `$SECRETS_DIR/token.jwt`, so nothing else has to be configured.
- The variables exist **only inside the pod**. For local development pass an
  explicit URL, or guard on `"MYSQL_HOST" in os.environ`.
- `MYSQL_*` is absent when the online feature store is disabled on the cluster
  or the project has no feature store service, and when the app was created with
  `db_access=False` (`hops app create --no-db-access`). Fail with a clear message
  instead of a `KeyError`.
- Injection happens before the app's own `env_vars`, so an app can override
  `MYSQL_DB` etc. to point at a different database if it must.

### Offline feature groups through Trino (same `db_access` knob)

When Trino is enabled on the cluster, the same flag also injects the identifiers
of the project's Trino coordinator, so an app in any language can read the
**offline** feature group tables with SQL (the Python SDK does this through
`project.get_trino_api()`, see **hops-trino-sql**):

| Variable | Value |
|---|---|
| `TRINO_HOST` / `TRINO_PORT` | internal DNS name and HTTPS port (`8443`) of the Trino coordinator |
| `TRINO_USER` | `<project>__<username>` of the person who **started** the app (the HDFS username, two underscores) |
| `TRINO_PASSWORD_SECRET_NAME` | name of the **Hopsworks secret** holding that user's Trino password (same string as `TRINO_USER`) |
| `TRINO_SCHEMA` | `<project>_featurestore`, the project's offline feature store schema |

There is deliberately **no `TRINO_CATALOG`**: the catalog depends on each feature
group's format, so pick it in the app.

- Same password rule as MySQL: read the secret through the SDK or the REST API
  with the job JWT. `TRINO_USER` and `MYSQL_USER` are different users with
  different secrets; do not mix them up.
- Tables are `<catalog>.<TRINO_SCHEMA>.<fg_name>_<version>`. The catalog is the
  feature group's format: `delta` for Delta groups, `hudi` for Hudi ones
  (`fg.time_travel_format` in the SDK, or the format shown on the feature group
  page). Either pass `catalog` when creating the client or fully qualify every
  table name. Trino's access rules give the app exactly the starter's project
  role (and the feature stores shared with the project).
- Trino is for scans and aggregations over the lakehouse tables. Primary-key
  lookups belong on the online tables through `MYSQL_*`.

### TLS to the platform from a non-Python app

`REST_ENDPOINT` and the Trino coordinator serve certificates signed by the
cluster CA, which is not in the image's system bundle. The pod gets the CA as
PEM at `$LIBHDFS_ROOT_CA_BUNDLE`, and `NODE_EXTRA_CA_CERTS` points at the same
file, so Node's `fetch` / `https` verify both with nothing to configure. Other
runtimes pass `$LIBHDFS_ROOT_CA_BUNDLE` as the CA file. Do not point
`SSL_CERT_FILE` or `REQUESTS_CA_BUNDLE` at it: those replace the system bundle
and the app's calls to public HTTPS APIs stop verifying.

### Privileges follow the project role of the user who starts the app

| Role of the starter | Grant on the project database |
|---|---|
| Data Owner | `ALL PRIVILEGES` — create tables, insert, update, delete |
| Data Scientist | `SELECT` only — reads work, any `CREATE TABLE` / `INSERT` fails |

An app that keeps state therefore has to be **started by a Data Owner**. If a
Data Scientist restarts it, the app comes up with a read-only connection.

### Table rules on RonDB

- RonDB is MySQL Cluster: tables default to `ENGINE=NDBCLUSTER`. Declare it
  explicitly so the table is never created as InnoDB by an unusual session default.
- Always define a `PRIMARY KEY` (NDB adds a hidden one otherwise, which makes
  updates and replication slower).
- Keep rows small: NDB stores at most ~30 000 bytes per row in-row; long
  `VARCHAR` columns count in full. Use `TEXT` / `JSON` / `BLOB` for large values,
  they are stored out of row.
- Prefer creating tables complete; adding foreign keys with `ALTER TABLE`
  afterwards is where NDB most often refuses a schema change.
- **Prefix app tables** (`app_<appname>_...`) so they cannot collide with feature
  group tables, which are named `<fg_name>_<version>`.
- Never write into a feature group table directly. Insert through
  `fg.insert()` so the offline store, statistics and validation stay in sync.
  Reading them with SQL is fine and is the fastest way to serve a dashboard.
- `hops sql` / Trino queries the **offline** lakehouse tables, not this database.

## Python

`PyMySQL` and SQLAlchemy ship with the `hopsworks` package, so nothing extra is
needed in `python-app-pipeline`.

```python
# db.py — one helper shared by Streamlit / FastAPI apps and agents
import os
import hopsworks
import pymysql

def mysql_settings() -> dict:
    try:
        host, db, user = (os.environ["MYSQL_HOST"], os.environ["MYSQL_DB"], os.environ["MYSQL_USER"])
    except KeyError as err:
        raise RuntimeError(
            f"{err.args[0]} is not set: the app was created with db_access=False, "
            "or this is not running inside a Hopsworks app/agent pod."
        ) from err
    password = os.environ.get("MYSQL_PASSWORD")          # explicit override, e.g. local dev
    if password is None:
        hopsworks.login()                                # in-cluster: JWT + certs, no prompt
        password = hopsworks.get_secrets_api().get(os.environ["MYSQL_PASSWORD_SECRET_NAME"])
    return dict(host=host, port=int(os.environ.get("MYSQL_PORT", "3306")),
                user=user, password=password, database=db)

def connect() -> pymysql.Connection:
    return pymysql.connect(**mysql_settings(), autocommit=True,
                           cursorclass=pymysql.cursors.DictCursor)

def sqlalchemy_url() -> str:
    s = mysql_settings()
    return f"mysql+pymysql://{s['user']}:{s['password']}@{s['host']}:{s['port']}/{s['database']}"
```

Plain PyMySQL:

```python
from db import connect

with connect() as conn, conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_reviews_notes (
          id         BIGINT AUTO_INCREMENT PRIMARY KEY,
          user_email VARCHAR(255) NOT NULL,
          note       TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=NDBCLUSTER DEFAULT CHARSET=utf8mb4
    """)
    cur.execute("INSERT INTO app_reviews_notes (user_email, note) VALUES (%s, %s)", (email, note))
    cur.execute("SELECT * FROM app_reviews_notes ORDER BY created_at DESC LIMIT 20")
    rows = cur.fetchall()
```

SQLAlchemy, cached once per Streamlit process (the secret lookup is a REST call,
do not repeat it on every rerun):

```python
import streamlit as st
from sqlalchemy import create_engine, text
from db import sqlalchemy_url

@st.cache_resource
def get_engine():
    return create_engine(sqlalchemy_url(), pool_pre_ping=True, pool_recycle=1800)

@st.cache_data(ttl=60)
def latest_transactions(n: int = 100):
    with get_engine().connect() as conn:          # read the online FG table directly
        return conn.execute(text("SELECT * FROM transactions_1 ORDER BY event_time DESC LIMIT :n"),
                            {"n": n}).mappings().all()
```

Agents: `hopsworks_agent_protocol.ManagedMemoryService()` already reads these
variables (see **hops-agent-deployment**); the helper above is the same logic for
an app's own tables.

## SQL

The statements the app runs. Anything a Data Scientist-started app runs must be
read-only.

```sql
-- app state table: explicit engine, explicit primary key, app_ prefix
CREATE TABLE IF NOT EXISTS app_dashboard_settings (
  user_email VARCHAR(255) NOT NULL,
  settings   JSON         NOT NULL,
  updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_email)
) ENGINE=NDBCLUSTER DEFAULT CHARSET=utf8mb4;

-- upsert
INSERT INTO app_dashboard_settings (user_email, settings) VALUES (?, ?)
  ON DUPLICATE KEY UPDATE settings = VALUES(settings);

-- read an online feature group (table = <fg_name>_<version>); primary-key lookups are the fast path
SELECT * FROM transactions_1 WHERE cc_num = ?;

-- what is in the project database
SHOW TABLES;
```

Reading feature group tables by their primary key is what the online store is
built for; large scans and aggregations belong in Trino over the offline tables
(**hops-trino-sql**).

## Node.js

A custom app (`app_kind="CUSTOM"`) can run Node. Two prerequisites:

1. **Node in the environment.** `python-app-pipeline` is a Python image; if
   `node` is not on the path, clone the environment and add the pip-installable
   `nodejs-bin` to its `app-requirements.txt` (see **hops-environments**), or
   build a custom image.
2. **The password.** Either resolve the secret with the Python SDK in the
   entrypoint and hand it to Node as `MYSQL_PASSWORD` (below), or read it from
   the REST API in Node: the launcher exports `NODE_EXTRA_CA_CERTS` for the
   cluster CA, so a plain `fetch` to `$REST_ENDPOINT` with the job JWT works. See
   [references/nodejs_secret_via_rest.md](references/nodejs_secret_via_rest.md).

```python
node_app = apps.create_app(
    name="node_api",
    app_kind="CUSTOM",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    entrypoint_command=(
        'bash -lc "'
        "export MYSQL_PASSWORD=$(python -c 'import os, hopsworks; hopsworks.login(); "
        "print(hopsworks.get_secrets_api().get(os.environ[\\\"MYSQL_PASSWORD_SECRET_NAME\\\"]))') && "
        'npm ci --omit=dev && exec node server.js"'
    ),
    app_port=8080,
    environment="node-app-env",     # the clone that has node
)
```

```js
// server.js — mysql2 pool over the injected variables; bind to 0.0.0.0:$APP_PORT
const express = require("express");
const mysql = require("mysql2/promise");

for (const v of ["MYSQL_HOST", "MYSQL_DB", "MYSQL_USER", "MYSQL_PASSWORD"]) {
  if (!process.env[v]) throw new Error(`${v} is not set: created with db_access=False, or not in a Hopsworks pod`);
}

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: Number(process.env.MYSQL_PORT || 3306),
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
  waitForConnections: true,
  connectionLimit: 5,
});

const app = express();
app.use(express.json());

app.get("/", (_req, res) => res.json({ status: "ok" }));          // readiness probe (custom apps probe "/")

app.get("/api/transactions/:ccNum", async (req, res) => {          // online FG table <fg>_<version>
  const [rows] = await pool.execute("SELECT * FROM transactions_1 WHERE cc_num = ?", [req.params.ccNum]);
  res.json(rows);
});

app.post("/api/notes", async (req, res) => {                        // app-owned table (needs a Data Owner starter)
  await pool.execute(
    `CREATE TABLE IF NOT EXISTS app_node_api_notes (
       id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     ) ENGINE=NDBCLUSTER DEFAULT CHARSET=utf8mb4`);
  await pool.execute("INSERT INTO app_node_api_notes (note) VALUES (?)", [req.body.note]);
  res.status(201).end();
});

app.listen(Number(process.env.APP_PORT), "0.0.0.0");
```

`package.json` needs `express` and `mysql2`. Because the browser reaches the app
under the Hopsworks proxy mount, build client-side URLs relative to the mount
(**hops-app**, "Routing and readiness").

### Offline feature groups from Node.js with Trino

`trino-client` (the official Trino Node client, HTTP under the hood) plus the
`TRINO_*` variables. The password is the Hopsworks secret named by
`TRINO_PASSWORD_SECRET_NAME`, read with the job JWT exactly like the MySQL one
(`readHopsworksSecret` is in
[references/nodejs_secret_via_rest.md](references/nodejs_secret_via_rest.md)).
`NODE_EXTRA_CA_CERTS` already makes the coordinator's certificate trusted.

```js
// trino.js — one client per process; the secret lookup is a REST call, do it once
import { Trino, BasicAuth } from "trino-client";
import { readHopsworksSecret } from "./secrets.js";

for (const v of ["TRINO_HOST", "TRINO_PORT", "TRINO_USER", "TRINO_PASSWORD_SECRET_NAME", "TRINO_SCHEMA"]) {
  if (!process.env[v]) throw new Error(`${v} is not set: db_access=False, Trino disabled on the cluster, or not in a Hopsworks pod`);
}

const password = await readHopsworksSecret(process.env.TRINO_PASSWORD_SECRET_NAME);
export const trino = Trino.create({
  server: `https://${process.env.TRINO_HOST}:${process.env.TRINO_PORT}`,
  catalog: "delta",                       // the feature groups' format: delta or hudi
  schema: process.env.TRINO_SCHEMA,
  auth: new BasicAuth(process.env.TRINO_USER, password),
});

// Collect a result set as [{column: value}]; results arrive in pages, the first page carries the columns.
export async function rows(sql) {
  const iter = await trino.query(sql);
  const out = [];
  let columns = null;
  for await (const page of iter) {
    if (page.error) throw new Error(`${page.error.errorName}: ${page.error.message}`);
    if (page.columns && !columns) columns = page.columns.map((c) => c.name);
    for (const row of page.data ?? []) out.push(Object.fromEntries(row.map((v, i) => [columns[i], v])));
  }
  return out;
}

// offline feature group table <fg_name>_<version> in the project schema; filter on the partition key
const recent = await rows(
  `SELECT cc_num, amount, event_time FROM transactions_1 WHERE event_time >= DATE '2025-01-01' LIMIT 100`);
```

Quote identifiers with double quotes and never interpolate user input into the
SQL text; `trino-client` has no bound parameters, so validate values first.

## Commands / API

```bash
hops app create <name> --path /Projects/<p>/Users/<u>/app.py --start   # db access on by default
hops app create <name> ... --no-db-access                              # opt out
hops app info <name>                                                   # "Database access" row
```

```python
apps.create_app(name=..., app_path=..., db_access=True)   # default
app.db_access
```

## Docs

- Online feature store / RonDB: https://docs.hopsworks.ai/latest/concepts/fs/feature_group/fg_overview/
- Secrets API: https://docs.hopsworks.ai/latest/user_guides/projects/secrets/create_secret/

## Related skills

- **hops-app** — writing, creating and running the app that uses this database.
- **hops-agent-deployment** — agent deployments get the same variables; `ManagedMemoryService` uses them.
- **hops-fg** / **hops-fv** — write features through the SDK, never straight into the online tables.
- **hops-trino-sql** — SQL over the offline tables for scans and aggregations.
- **hops-environments** — clone `python-app-pipeline` to add Node or extra drivers.
