# Reading Hopsworks data from an app

Part of **hops-app**: read when the app reads feature groups, a model or a deployment.

## The default read path

An app reads its feature groups through Trino with the SDK's connection, one bounded query per
request, as the skeleton's `_connection()` does:

```python
project = hopsworks.login()
verify = os.environ.get("HOPSWORKS_HOSTNAME_VERIFICATION", "true").lower() != "false"
conn = project.get_trino_api().connect(
    catalog="delta",  # hudi or iceberg for a feature group in that format
    schema=f"{project.name.lower()}_featurestore",
    verify=verify,  # off only where the client turns it off (a laptop, a build sandbox)
)
```

A feature group is the table `<name>_<version>`; values are bound as `?` parameters (the query
rules are in [hops-app-built.md](hops-app-built.md), **Each request reads only what it returns**).
Read through the SDK instead only for what Trino cannot serve: a feature group created moments
ago, an online feature vector by primary key, an embedded model read through its feature view,
and app state in the online store (below). A JavaScript app reaches Trino through its own Node
server (Reading Trino from a JavaScript app, below).

## When the SDK reads instead

The SDK calls are the same as anywhere (hops-fg / hops-fv / hops-online-inference);
three things are app-specific:

- **A just-created feature group is not queryable via Trino/`hops sql` immediately.** The offline table syncs into the Trino catalog with a short lag, so a `SELECT ... FROM <fresh_fg>` right after `insert` can return `TABLE_NOT_FOUND`. Online feature-vector reads are available before the Trino table is, so make the app not-found-safe (warn on an empty online vector) instead of trusting a range from a fresh query.
- **Embedded model** (predict locally instead of calling a deployment): `model_dir = mr.get_model("fraud_model", version=1).download()`; cache the loaded model and its feature view in `@st.cache_resource` so the download happens once, and read through the feature view so the same MDTs/ODTs the model saw in training are applied (no training/serving skew).
- **An app over a batch ML system** reads the prediction feature group the batch job writes and
  never calls a deployment, because a batch system has none; a what-if score uses the embedded
  model above.
- **Calling a deployment** (real-time systems only): check `deployment.is_running()` before `predict`, and surface a message rather than blocking ([frameworks.md](frameworks.md), Streamlit caching and cold start).
- **App state and SQL over the online store:** the pod gets `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DB`, `MYSQL_USER` and `MYSQL_PASSWORD_SECRET_NAME` for the project's online feature store database (created on demand at start). Keep app tables there with an `app_` prefix and read online feature group tables (`<fg>_<version>`) by primary key. Connection helpers for Python, SQL and Node.js, privileges per role, and RonDB table rules: **hops-app-db**.

## Reading Trino from a JavaScript app

The browser cannot query Trino itself: `trino.service.consul` resolves only
inside the cluster, Trino takes passwords only over HTTPS (port 8080 answers
`403 Authentication over HTTP is not enabled`), and the password must not reach
the page. A Node server in the app holds the connection and answers the UI's
relative `fetch("api/...")` calls with fixed queries, so the browser sends
parameters, never SQL. `python-agent-pipeline` ships Node.

The connection comes from the SDK: `trino_env.py` logs in, which writes the
cluster CA to `/tmp/ca_chain.pem`, and writes the coordinator URL, the user
`<project>__<username>`, its password from the secrets store and the schema
`<project>_featurestore` to `/tmp/trino.json`, readable only by the app's user.
Never print the password or export it as an environment variable: both leak it
into logs and every child process. A feature group is the table
`<name>_<version>` in catalog `delta` (or `hudi`/`iceberg`, per its format).

```javascript
import { readFileSync } from "node:fs";
import pkg from "trino-client";
const { Trino, BasicAuth } = pkg;

const conn = JSON.parse(readFileSync("/tmp/trino.json", "utf8")); // from trino_env.py
const trino = Trino.create({
  server: conn.server, // https://coordinator.trino.service.consul:8443
  catalog: "delta",
  schema: conn.schema, // <project>_featurestore
  auth: new BasicAuth(conn.user, conn.password),
  ssl: { ca: readFileSync(conn.ca) }, // the cluster CA, not in Node's trust store
});

const query = await trino.query("SELECT * FROM customers_1 LIMIT 100");
for await (const result of query) {
  if (result.error) throw new Error(result.error.message);
  console.log(result.columns?.map((c) => c.name), result.data); // rows arrive in pages
}
```

The whole app (server with `/health`, `/api/rows` and the static UI) is
[references/trino_js/](trino_js/). Install `trino-client` into the
app directory before the upload, so the app starts without the network, and
copy `app.css` from the skeleton (`<hops-app>` is the directory of the path `hops skills info hops-app` prints):

```bash
cp -r <hops-app>/references/trino_js apps/customers
cp <hops-app>/references/app_skeleton/static/app.css apps/customers/static/
(cd apps/customers && npm install --no-audit --no-fund)
hops files upload apps/customers Users/<user>/apps/ --overwrite
hops app create customers --path /Projects/<project>/Users/<user>/apps/customers/server.js \
  --app-kind custom --app-port 8080 --readiness-probe-path /health \
  --env python-agent-pipeline --start \
  --entrypoint-command "bash -lc 'python trino_env.py && APP_TABLE=customers_1 exec node server.js'"
```
