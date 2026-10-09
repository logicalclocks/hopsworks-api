# `@hopsworks/app` — Hopsworks from a Node.js app

Pre-installed in the `python-app-pipeline` image (and any environment cloned
from it), importable from anywhere in the pod without a `package.json` entry.
It reads only the environment the Hopsworks launcher injects, so a Node app
needs no configuration to reach the platform.

```js
import { getSecret, mysqlConfig, trinoClient, query, streamQuery, featureGroupTable, inHopsworks } from "@hopsworks/app";
```

| Function | Returns | Needs |
|---|---|---|
| `inHopsworks()` | `true` inside a Hopsworks app pod | — |
| `getSecret(name)` | a private secret of the user who **started** the app (cached per process) | `REST_ENDPOINT`, `SECRETS_DIR` |
| `restCall(path, init?)` | parsed JSON of a Hopsworks REST call as that user; `path` is relative to `/hopsworks-api/api` | same |
| `mysqlConfig()` | `{ host, port, user, password, database }` of the project's online feature store, for `mysql2` / `mysql` / `knex` | `MYSQL_*` (`db_access=True`) |
| `trinoClient({ catalog, schema? })` | an authenticated `TrinoClient` (Trino REST protocol, no dependency) for the offline feature groups | `TRINO_*` (Trino enabled; independent of `db_access`) |
| `query(client, sql)` | all rows as `[{ column: value }]` | — |
| `streamQuery(client, sql)` | async iterator of row objects, page by page | — |
| `featureGroupTable(name, version, { catalog, schema? })` | `"<catalog>"."<schema>"."<name>_<version>"`, quoted | `TRINO_SCHEMA` unless `schema` given |

Every function throws `HopsworksEnvError` (with `.variable`) when a variable is
missing, so the error says whether the app was created with `db_access=False`
(MySQL), Trino is disabled on the cluster (Trino), or the code is running
outside a pod. Passwords are never in
the environment: they are read once over REST with the pod's job JWT.

## Secrets

Any private secret of the starting user, for example a third-party API key
stored with `hops secret create` or the UI:

```js
const openaiKey = await getSecret("openai_api_key");
```

`MYSQL_PASSWORD_SECRET_NAME` and `TRINO_PASSWORD_SECRET_NAME` are such secrets
too; `mysqlConfig()` and `trinoClient()` read them for you.

## Online feature store (MySQL / RonDB)

```js
import mysql from "mysql2/promise";
import { mysqlConfig } from "@hopsworks/app";

const pool = mysql.createPool({ ...(await mysqlConfig()), waitForConnections: true, connectionLimit: 5 });
const [rows] = await pool.execute("SELECT * FROM transactions_1 WHERE cc_num = ?", [ccNum]);
```

`mysqlConfig()` honours an explicit `MYSQL_PASSWORD` (local development, or an
entrypoint that resolved it with the Python SDK) and otherwise reads the secret.
The privileges follow the starter's project role (**hops-app-db**, "Privileges").

## Offline feature groups (Trino)

```js
import { trinoClient, query, streamQuery, featureGroupTable } from "@hopsworks/app";

const trino = await trinoClient({ catalog: "delta" });        // catalog = the feature group's format: delta, hudi, iceberg

// <fg_name>_<version> in the project's schema (TRINO_SCHEMA, "<project>_featurestore")
const recent = await query(trino,
  `SELECT cc_num, amount, event_time FROM transactions_1 WHERE event_time >= DATE '2025-01-01' LIMIT 100`);

// another format or a shared feature store: fully qualified
const n = await query(trino,
  `SELECT count(*) AS n FROM ${featureGroupTable("transactions", 1, { catalog: "hudi", schema: "other_featurestore" })}`);

// results too large to hold: stream
for await (const row of streamQuery(trino, "SELECT * FROM transactions_1")) handle(row);
```

- The catalog is required on purpose. It depends on each feature group's
  format (`fg.time_travel_format` in the SDK, or the feature group page), and a
  project can mix formats, so there is no default that is right for every table.
- `trinoClient()` passes `source`, `session`, `extraCredential` and
  `extraHeaders` through to the protocol. `client.query(sql)` yields the raw
  result pages if you need `stats` or the query `id`.
- BIGINT values above 2^53 arrive as `BigInt` (JSON would round them); call
  `Number()` or `String()` where you need the other type. DECIMAL arrives as a
  string, as Trino sends it.
- Leaving a `streamQuery()` loop early cancels the query on the coordinator, so
  paging a UI over a large result does not leave work running.
- Trino's HTTP protocol has no bound parameters. Quote identifiers with double
  quotes and never interpolate user input into the SQL text; validate first.
- One client per process: creating it is the single REST call for the secret.
  Expect 100–300 ms per query for the coordinator round trip; aggregate in SQL
  and page results rather than pulling large tables into Node.

## Local development

```js
import { inHopsworks, mysqlConfig } from "@hopsworks/app";

const db = inHopsworks()
  ? await mysqlConfig()
  : { host: "127.0.0.1", port: 3306, user: "dev", password: process.env.DEV_DB_PASSWORD, database: "dev" };
```

Outside a pod install it like any package for the editor and tests:
`npm install <path-to-hopsworks-app-tarball>`; in the pod the global copy wins
only when the app has no `node_modules/@hopsworks/app` of its own.

## CommonJS

The module is ESM without top-level await, so on the image's Node 26 a
CommonJS app can `const { mysqlConfig } = require("@hopsworks/app");`.

## TLS to the platform

`REST_ENDPOINT` and the Trino coordinator present certificates signed by the
cluster CA, which is not in the image's system bundle. The pod carries the CA
as PEM at `$LIBHDFS_ROOT_CA_BUNDLE`, and `NODE_EXTRA_CA_CERTS` points at the
same file, so Node's `fetch`, `https` and therefore `@hopsworks/app` verify
both with nothing to configure. On a Hopsworks version whose pods do not carry
these variables yet, set it yourself from the file the launcher already wrote:

```bash
export NODE_EXTRA_CA_CERTS="$PEMS_DIR/${HADOOP_USER_NAME}_root_ca.pem"
exec node server.js
```

Do not point `SSL_CERT_FILE` or `REQUESTS_CA_BUNDLE` at this file: those replace
the system bundle instead of extending it, and the app's calls to public HTTPS
APIs stop verifying.

## Without the module

What `getSecret()` does, for an image without `@hopsworks/app` or another
runtime: the pod authenticates with the job JWT mounted at
`$SECRETS_DIR/token.jwt`, the API base is `$REST_ENDPOINT/hopsworks-api/api`,
and a private secret is `GET /users/secrets/<name>`, answering
`{"items": [{"name": ..., "secret": ...}]}`.

```js
import { readFileSync } from "node:fs";
import { join } from "node:path";

export async function readHopsworksSecret(name) {
  const token = readFileSync(join(process.env.SECRETS_DIR, "token.jwt"), "utf8").trim();
  const url = `${process.env.REST_ENDPOINT}/hopsworks-api/api/users/secrets/${encodeURIComponent(name)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) throw new Error(`secret ${name}: HTTP ${res.status}`);
  return (await res.json()).items[0].secret;
}
```

Cache the result; the lookup is one REST call per process start. The JWT is
renewed by the platform in place, so re-read the file if a call returns 401.
