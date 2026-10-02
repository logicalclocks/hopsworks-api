/*
A JavaScript app that reads a feature group's offline table through Trino.

The browser never talks to Trino: trino.service.consul only resolves inside
the cluster, Trino takes passwords only over HTTPS, and the password must not
reach the page. This Node server holds the connection and answers the UI's
fetch("api/...") calls with fixed queries; the browser sends parameters,
never SQL. Start it with the connection exported by trino_env.py:

  bash -lc 'eval "$(python trino_env.py)" && exec node server.js'
*/
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join } from "node:path";
import pkg from "trino-client";

const { Trino, BasicAuth } = pkg;

const trino = Trino.create({
  server: process.env.TRINO_SERVER, // https://coordinator.trino.service.consul:8443
  catalog: "delta",
  schema: process.env.TRINO_SCHEMA, // <project>_featurestore
  auth: new BasicAuth(process.env.TRINO_USER, process.env.TRINO_PASSWORD),
  // The cluster CA signs Trino's certificate and is not in Node's trust store.
  ssl: { ca: readFileSync(process.env.TRINO_CA) },
});

// A feature group is the table <name>_<version>; set APP_TABLE to read another.
const TABLE = process.env.APP_TABLE || "customers_1";
if (!/^[a-z_][a-z0-9_]*$/.test(TABLE)) throw new Error(`APP_TABLE ${TABLE} is not a table name`);

// Trino answers in pages: each result has data (rows as arrays) and columns.
async function rows(sql) {
  const query = await trino.query(sql);
  const out = [];
  let columns;
  for await (const result of query) {
    if (result.error) throw new Error(result.error.message);
    columns ??= result.columns?.map((column) => column.name);
    out.push(...(result.data ?? []));
  }
  return { columns: columns ?? [], rows: out };
}

const routes = {
  "/api/rows": (params) => {
    const limit = Math.min(Math.max(Number.parseInt(params.get("limit") ?? "100", 10) || 100, 1), 1000);
    return rows(`SELECT * FROM ${TABLE} LIMIT ${limit}`);
  },
};

const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css" };
const STATIC = join(import.meta.dirname, "static");

function send(response, status, type, body) {
  response.writeHead(status, { "Content-Type": `${type}; charset=utf-8` });
  response.end(body);
}

createServer(async (request, response) => {
  const url = new URL(request.url, "http://app");
  try {
    if (url.pathname === "/health") return send(response, 200, "application/json", '{"status":"ok"}');
    const route = routes[url.pathname];
    if (route) return send(response, 200, "application/json", JSON.stringify(await route(url.searchParams)));
    const file = url.pathname === "/" ? "index.html" : url.pathname.replace(/^\/static\//, "");
    if (!/^[\w.-]+$/.test(file)) return send(response, 404, "text/plain", "not found");
    return send(response, 200, TYPES[extname(file)] ?? "text/plain", readFileSync(join(STATIC, file)));
  } catch (error) {
    const status = error.code === "ENOENT" ? 404 : 500;
    return send(response, status, "application/json", JSON.stringify({ detail: error.message }));
  }
}).listen(Number(process.env.APP_PORT || 8080), "0.0.0.0");
