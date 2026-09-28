# Resolving the MySQL password from Node.js without the Python SDK

The pod authenticates to the Hopsworks REST API with the job JWT that the
platform mounts at `$SECRETS_DIR/token.jwt`. The API base is
`$REST_ENDPOINT/hopsworks-api/api`, and a private secret is read with
`GET /users/secrets/<name>`, which answers `{"items": [{"name": ..., "secret": ...}]}`.

`REST_ENDPOINT` is an internal `https://` address signed by the cluster CA. The
pod only carries that CA as a Java truststore (`$DOMAIN_CA_TRUSTSTORE`), so
either export it as PEM once in the entrypoint and point Node at it, or resolve
the password in the entrypoint with the Python SDK as the main skill shows
(simplest, and what agent deployments do).

Export the CA as PEM in the entrypoint (the Python SDK writes it on login):

```bash
python -c 'import hopsworks; hopsworks.login()'      # materialises /tmp/ca_chain.pem
export NODE_EXTRA_CA_CERTS=/tmp/ca_chain.pem
exec node server.js
```

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
