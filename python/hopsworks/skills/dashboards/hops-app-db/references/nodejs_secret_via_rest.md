# Resolving the MySQL password from Node.js without the Python SDK

The pod authenticates to the Hopsworks REST API with the job JWT that the
platform mounts at `$SECRETS_DIR/token.jwt`. The API base is
`$REST_ENDPOINT/hopsworks-api/api`, and a private secret is read with
`GET /users/secrets/<name>`, which answers `{"items": [{"name": ..., "secret": ...}]}`.

`REST_ENDPOINT` is an internal `https://` address signed by the cluster CA, which
is not in the image's system bundle. The platform mounts that CA as a Java
truststore, converts it to PEM before the entrypoint runs, and sets two pod
environment variables for it:

- `LIBHDFS_ROOT_CA_BUNDLE` — the PEM file, `$PEMS_DIR/${HADOOP_USER_NAME}_root_ca.pem`,
  for any HTTP client that takes a CA file.
- `NODE_EXTRA_CA_CERTS` — the same file; Node adds it to its default trust store at
  startup, so `fetch` / `https` verify the endpoint with nothing else to configure.

Nothing to do in the entrypoint; `exec node server.js` is enough. On a Hopsworks
version whose pods do not carry these variables yet, set the variable yourself
from the file the launcher already wrote:

```bash
export NODE_EXTRA_CA_CERTS="$PEMS_DIR/${HADOOP_USER_NAME}_root_ca.pem"
exec node server.js
```

Do not point `SSL_CERT_FILE` or `REQUESTS_CA_BUNDLE` at this file: those replace
the system bundle instead of extending it, and the app's calls to public HTTPS
APIs would stop verifying.

Then in Node:

```js
const fs = require("fs");
const path = require("path");

async function readHopsworksSecret(name) {
  const token = fs.readFileSync(path.join(process.env.SECRETS_DIR, "token.jwt"), "utf8").trim();
  const url = `${process.env.REST_ENDPOINT}/hopsworks-api/api/users/secrets/${encodeURIComponent(name)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) throw new Error(`secret ${name}: HTTP ${res.status}`);
  const body = await res.json();
  return body.items[0].secret;
}

module.exports = { readHopsworksSecret };   // also used for TRINO_PASSWORD_SECRET_NAME

async function mysqlConfig() {
  const password = process.env.MYSQL_PASSWORD
    ?? await readHopsworksSecret(process.env.MYSQL_PASSWORD_SECRET_NAME);
  return {
    host: process.env.MYSQL_HOST,
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER,
    password,
    database: process.env.MYSQL_DB,
  };
}

module.exports = { mysqlConfig };
```

Cache the result: the secret lookup is one REST call per process start, not per
request. The JWT is renewed by the platform in place, so re-read the file if a
call ever returns 401.
