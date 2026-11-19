# Apps built by `/hops app`

Part of **hops-app**: the contract **hops-app-builder** follows, read for every app made from the skeleton.

`/hops app` turns a description in natural language into a running app from
[references/app_skeleton/](app_skeleton/):

- **Shape.** A `custom` app: `app.py` (FastAPI) serves a JSON API under `/api`
  and a static UI from `static/` (`index.html`, `app.js` as plain ES modules,
  `app.css`) in one process on `0.0.0.0:$APP_PORT`, with `/health` for the
  readiness probe. No build step and no CDN, because the app runs air-gapped;
  every URL in the UI is relative (`fetch("api/top")`, `src="static/app.js"`),
  and `index.html` adds the trailing slash relative URLs need under the proxy
  mount. A framework that needs a build (React, Vue, Svelte) only for a
  git-backed app whose repository can hold the built assets. Streamlit when the
  description is a data view with no custom interaction.
- **Look.** The skeleton's `app.css` carries the Hopsworks tokens; extend it,
  following [hops-design](../../hops-design/SKILL.md) (flat surfaces, records in
  tables or lists, stats in one row, no shadows, light theme only).
- **Nothing moves on interaction.** A list or panel that reloads (a filter, a
  selection) keeps its current content, dimmed with `aria-busy`, until the new
  content replaces it; placeholders only on the first load. The skeleton's
  `app.css` reserves the scrollbar gutter, so content that shrinks for a moment
  never shifts the page sideways.
- **Charts are Chart.js**, vendored in `static/vendor/chart.umd.min.js` (MIT,
  licence beside it) and themed once by `static/chart-theme.js` (hops-design
  tokens, no legend for one series, area fills, padded ticks). A chart sets only
  its data, labels and axis titles, inside a `.chart` box that fixes its height;
  the skeleton draws a line over time and bars by category. Never hand-drawn SVG,
  never a CDN.
- **Nothing spills sideways.** A table goes in the skeleton's `.table-wrap`, which
  scrolls it inside its own frame (`contain: inline-size` keeps its width out of
  the layout around it), and every grid the app adds gives its columns
  `minmax(0, …)` (`grid-template-columns: minmax(0, 1fr)`): a plain `1fr` or
  implicit column grows to its widest content and pushes the page past the right
  edge of the Hopsworks app panel. Check the app at about 600px and 900px wide
  ([look.md](look.md)).
- **The description is the module docstring** of `app.py`, updated on every
  edit, so the next edit starts from what the app is now.
- **Each request reads only what it returns.** The skeleton's routes query the
  feature group through Trino: the latest row per entity with
  `ROW_NUMBER() OVER (PARTITION BY <id> ORDER BY <ts> DESC)`, the top k with
  `ORDER BY ... LIMIT ?`, one entity with `WHERE <id> = ? ORDER BY <ts> DESC
  LIMIT 1`. Values are bound as `?` parameters, never formatted into the SQL;
  `limit` is capped (`MAX_LIMIT`), and results are cached for `CACHE_SECONDS`.
  Never read a whole feature group into pandas to sort or filter it per request.
- **Where the source lives.** `Users/<user>/apps/<name>/` (`~/apps/<name>/` in a
  terminal; `./apps/<name>/` from a laptop, uploaded by `hops app deploy`),
  or a git-backed app when the working directory is a GitHub repository
  (`--git-url`, `--git-branch`, `--git-auto-redeploy`, so a push is a
  redeploy). An ML system's app keeps its source in the system's repository
  under `<slug>/app/`.
- **Environment.** `python-agent-pipeline` ships FastAPI and uvicorn. A library
  it lacks goes into `app-requirements.txt`; `hops app deploy` then runs the app
  in a clone `<name>-app-env` with the file installed.

One command deploys, from a laptop or a terminal, for a new app and for every
change after it:

```bash
hops app deploy customer-lookup apps/customer-lookup
```

It uploads the directory to `Users/<user>/apps/customer-lookup/` (missing
folders created, files overwritten, dotfiles and `__pycache__` skipped), clones
`python-agent-pipeline` to `customer-lookup-app-env` when the directory has
`app-requirements.txt` (installed again only when the file changed), creates the
app (`app.py` as a custom app on port 8080, readiness on `/health`) or redeploys
it, and returns once the new execution serves, the URL on the last line. A
failure exits non-zero and names the next step. `--env` picks another
environment, `--timeout` the seconds to wait (600), `--check <route>` fetches one
route of the deployed app through the Hopsworks proxy and fails the deploy on a
non-2xx answer, and `--json` prints the app and each file's sha256.

**Smoke tests** go to the deployed app with `hops app deploy <name> <dir> --check
<route>`, one API route that reads data (`api/top`): it passes through the proxy
with a token minted from the API key, which a plain request with the key does
not (401). `hops app deploy` returning means the readiness probe on `/health`
passed on the new files; `--check` means the data answers too. In a Hopsworks terminal, whose `kubectl`
reaches the project namespace, then call the page and each API route the
description implies:

```bash
POD=$(kubectl get pods -o name | grep customer-lookup)
kubectl port-forward "$POD" 18080:8080 &
curl -s localhost:18080/health; curl -s localhost:18080/ | head; curl -s localhost:18080/api/top
```

From an external client without `kubectl`, the readiness probe is the smoke
test, and the URL is handed to the user to open.
