# Plan: Port `hops` CLI from Go to Python (in-tree)

Target: replace the Go `hopsworks-cli` (`hops` binary) with a Python CLI that ships as part of the `hopsworks` pip package, built on top of the existing Python SDK, with REST fall-throughs for anything the SDK does not expose. The CLI is consumed by three terminal images in `docker-images` (`terminal-server`, `terminal-gpu`, `terminal-spark`), which currently `git clone + go build` the Go CLI into `/usr/local/bin/hops`.

## 1. Background and constraints

- **Go CLI** (`../hopsworks-cli`, v0.8.8): 7.3k LoC, Cobra-based, installs as `hops`. Command surface in section 3. Ships in all 3 terminal images.
- **Go CLI already delegates to Python SDK** via subprocess for anything non-trivial (FG insert, FG create, FG derive, FV create, TD compute/read, model register/download, embeddings search). HTTP client is used mainly for: listing, preview, deployments, jobs, connectors, charts. So a Python reimplementation removes the subprocess hop entirely for the hard cases — it just calls the SDK.
- **Existing Python CLI plumbing**: `python/pyproject.toml` already declares a console script `hopsworks-mcp = "hopsworks.mcp:run_server_command"`. The MCP server uses **Click**, and the tool surface under `python/hopsworks/mcp/tools/` (auth, project, jobs, feature_group, dataset, brewer) is a head start — those modules already wrap SDK calls in a way that maps 1:1 to CLI subcommands.
- **Python SDK login semantics** (`python/hopsworks/__init__.py:81`): `hopsworks.login(host, port, project, api_key_value, api_key_file, …)`, reads env (`HOPSWORKS_HOST`, `HOPSWORKS_PORT`, `HOPSWORKS_PROJECT`, `HOPSWORKS_API_KEY`, `HOPSWORKS_HOSTNAME_VERIFICATION`, `HOPSWORKS_TRUST_STORE_PATH`, `HOPSWORKS_CERT_FOLDER`, `HOPSWORKS_ENGINE`); caches SaaS key at `~/.{username}_hopsworks_app/.hw_api_key`. Inside a Hopsworks pod, `REST_ENDPOINT` triggers auto-login (no key needed).
- **Go CLI config**: `~/.hops/config` (YAML), plus internal mode from `REST_ENDPOINT` + `SECRETS_DIR/token.jwt`. The new CLI must read the same env vars *and* continue to honor `~/.hops/config` so existing user setups keep working, while also consuming `HOPSWORKS_*` env vars.
- **Repo rules** (`.claude/CLAUDE.md`): HTTP calls belong in `core/<entity>_api.py`, public entities use `@public` from `hopsworks_apigen`, annotation-only imports inside `TYPE_CHECKING`, optional packages gated by `@uses_polars`-style decorators, `hopsworks_common` must not runtime-import `hopsworks`/`hsfs`/`hsml`. Docstrings one sentence per line, Google style.
- **Commands** already in CLAUDE.md: `uv run --project python ruff check --fix python/`, `uv run --project python pytest python/tests`, `uv run --project python docsig …`.

## 2. High-level design

### 2.1 Package layout

```
python/hopsworks/cli/
├── __init__.py
├── __main__.py              # `python -m hopsworks.cli`
├── main.py                  # entry point; assembles the Click app
├── config.py                # ~/.hops/config read/write; env+file merge; mode detection
├── auth.py                  # login flow; reuses hopsworks.login(); caches host/project/fs_id
├── output.py                # table vs JSON; TTY-aware color; `--json` global flag
├── client.py                # thin REST escape hatch for endpoints SDK doesn't wrap
├── py_bridge.py             # in-process SDK calls (NO subprocess); shared Project/FS/MR handles
├── commands/
│   ├── __init__.py
│   ├── login.py             # hops login
│   ├── project.py           # hops project list|use|info
│   ├── files.py             # hops files list|mkdir|upload|download|share|unshare|remove (HopsFS file system)
│   ├── fg.py                # hops fg list|info|preview|features|stats|keywords|…|insert|derive|search
│   ├── fg_external.py       # hops fg create-external
│   ├── fv.py                # hops fv list|info|create|get|read|delete
│   ├── datasource.py        # hops datasource list|info|test|databases|tables|preview|create|delete
│   ├── td.py                # hops td list|create|compute|read|delete
│   ├── transformation.py    # hops transformation list|create
│   ├── model.py             # hops model list|info|register|download|delete
│   ├── deployment.py        # hops deployment list|info|create|start|stop|predict|logs|delete
│   ├── job.py               # hops job list|info|create|run|stop|status|logs|history|delete|schedule…
│   ├── context.py           # hops context  (LLM dump)
│   ├── setup.py             # hops setup    (Modal-style browser token flow — PRIMARY auth)
│   ├── init.py              # hops init     (Claude Code skill/permissions scaffold)
│   └── update.py            # hops update   (prints pip upgrade instructions)
└── templates/
    └── SKILL.md             # embedded via importlib.resources

python/tests/cli/
├── test_config.py
├── test_output.py
├── test_fg_commands.py      # Click CliRunner + mocked project API
├── test_job_commands.py
└── …
```

Rationale for living inside `python/hopsworks/cli/` (not `python/hopsworks_common/`): `hopsworks_common` is forbidden from runtime-importing `hopsworks`/`hsfs`/`hsml`, but the CLI explicitly needs all three. The `hopsworks` package is the only correct home.

### 2.2 Console scripts (in `pyproject.toml`)

```toml
[project.scripts]
hopsworks-mcp = "hopsworks.mcp:run_server_command"
hops = "hopsworks.cli.main:main"
hopsworks = "hopsworks.cli.main:main"   # discoverability alias
```

Keep the name `hops` for drop-in replacement in terminal images. Add `hopsworks` alias so `pip install hopsworks` produces a CLI named after the package (future-proof).

### 2.3 CLI framework choice — **Click**

Click is already a dep (via `mcp` extras) and used in `mcp/run_server.py`. Adds zero new dependency burden if we promote `click` into base deps. Alternative (Typer) builds on Click and would work but is not used elsewhere — avoid to keep the dep graph flat.

For tables we use plain text with ANSI codes mirroring the Go `pkg/output` module; no `rich` or `tabulate` unless we discover we need wrapping/truncation. If we do, prefer `rich` since Claude Code already renders it well and a few SDK error paths already use `tqdm`.

### 2.4 Auth + config model

**Primary auth UX is `hops setup`** — see section 10 for the full browser token flow.
`hops login` remains as a non-interactive/scripted fallback for flag-driven auth.

Config file: **`~/.hops.toml`** (Modal-style), e.g.

```toml
[default]
host = "https://c.app.hopsworks.ai"
api_key = "XXXX.YYYY"
api_key_name = "dowling-jim"
project = "my_project"
project_id = 119
feature_store_id = 67
```

Multi-host support via TOML tables (`[default]`, `[<profile>]`) reserved for later; initial
implementation just reads/writes `[default]`. Migration: if `~/.hops/config` (the Go-CLI YAML) exists and
`~/.hops.toml` does not, auto-migrate on first run and leave a one-line note on stderr. Read via
stdlib `tomllib` (Python 3.11+); write via `tomli-w` (pure-Python, ~200 LoC dependency) — or hand-serialize
since the schema is flat and small.

Resolution precedence (highest first):

1. CLI flags (`--host`, `--api-key`, `--project`, `--profile`)
2. `HOPSWORKS_*` env vars (official SDK names)
3. `REST_ENDPOINT` / `HOPSWORKS_API_KEY` / `PROJECT_NAME` / `HOPSWORKS_PROJECT_ID` (Go-CLI / in-pod names — kept for back-compat with existing terminal images)
4. `~/.hops.toml`
5. `~/.{username}_hopsworks_app/.hw_api_key` (SDK SaaS cache, read-only fallback)

Internal-mode detection is identical to Go: `REST_ENDPOINT` set *and* `SECRETS_DIR/token.jwt` readable → use JWT, skip `hops setup` entirely, resolve project from `PROJECT_NAME`/`HOPSWORKS_PROJECT_ID`.

`hops login` flow (fallback, keep for scripts):
- If external and no API key: interactive prompt (host, API key via `click.prompt(..., hide_input=True)`).
- Calls `hopsworks.login(host=…, api_key_value=…, project=…)`. On success, writes `~/.hops.toml`.
- Unlike `hops setup`, the user must already have an API key — no browser flow, no key creation.

`hops project use <name>` resolves `project_id` and `feature_store_id` by calling `hopsworks.login(project=name)` and stashing them in `~/.hops.toml`.

### 2.5 In-process vs subprocess

The Go CLI shells out to `python3 -c "<script>"` for anything that touches the SDK. In Python we just call the SDK directly in the same process. Benefits:

- **One login**, no `.hw_api_key` round-trip, no re-import of `hopsworks`/`hsfs` per command.
- Real Python tracebacks on failure, not opaque exit codes.
- Deletes ~1500 LoC of string-templated Python scripts from the Go codebase.

All CLI commands use a single lazy-initialized `get_project()` helper that calls `hopsworks.login(**resolved_config)` once per invocation, then dispatches. Click's `ctx.obj` carries the `Project` object across subcommands so nested groups don't re-login.

### 2.6 REST escape hatch

For the handful of endpoints the SDK doesn't wrap (connector `data_source/databases|tables|data`, deployment `logs`, deployment `action=start/stop`, chart/dashboard CRUD, variable API, admin endpoints), we reuse `hopsworks_common.client` that the SDK already authenticates. This gives:

- Shared session, auth, TLS config.
- No re-implementation of headers / base URL construction.
- Obeys repo rule "HTTP calls belong in `core/<entity>_api.py`" — add a `chart_api.py`, `dashboard_api.py` etc. under `python/hopsworks_common/core/` for any truly new REST surface, rather than putting raw calls in the CLI module.

### 2.7 Output

`output.py` provides:

- `print_table(headers, rows)` — TTY-aware bold/dim ANSI; falls back to plain text when `not sys.stdout.isatty()`.
- `print_json(obj)` — `json.dumps(..., default=str, indent=2)`.
- A module-level `JSON_MODE` flag flipped by the global `--json` / `-o json` option.
- Success/info/warn helpers that are silent when `JSON_MODE` is true (matches Go).

Support both `--json` (Go compatibility) and `-o json|table` (future YAML). Default is table.

### 2.8 `hops context` and `hops init`

- **`context`**: walk `project.get_feature_store()` → list FGs/FVs/jobs, emit Markdown (for Claude) or JSON. One-to-one port; no subprocess needed.
- **`init`**: writes `.claude/skills/hops/SKILL.md` from `importlib.resources.files("hopsworks.cli.templates") / "SKILL.md"` and patches `.claude/settings.local.json` to allow `Bash(hops *)`. Small JSON-edit routine; well-defined.

## 3. Command surface (target = 100% parity with Go v0.8.8)

Grouped by parent Click group. Flags follow Go names exactly to preserve muscle memory and docs.

| Group | Subcommands |
|---|---|
| `setup` | **primary auth (see §10)**; browser token flow; flags `--host`, `--force`, `--key-name`, `--no-browser`, `--timeout`. |
| `login` | scripted fallback; flags `--host`, `--api-key`, `--project`, `--save-key`. |
| `project` | `list`, `use <name>`, `info`. |
| `fs` | `list`. |
| `fg` | `list`, `info <name> [--version]`, `preview <name> [--n] [--online]`, `features <name>`, `stats <name> [--compute] [--features]`, `keywords <name>`, `add-keyword <name> <kw...>`, `remove-keyword <name> <kw>`, `search <name> --vector --k`, `create <name> --primary-key --features --event-time --online --embedding --description`, `create-external <name> --connector --query --primary-key` (own file), `insert <name> --file|--generate|--online [stdin]`, `derive <name> --base --join --primary-key …`, `delete <name> [--version]`. |
| `connector` (alias `conn`) | `list`, `info <name>`, `test <name>`, `databases <name>`, `tables <name> [--database]`, `preview <name> --table`, `create snowflake|jdbc|s3|bigquery …`, `delete <name>`. |
| `fv` | `list`, `info <name>`, `create <name> --feature-group|--join --transform --labels --features --description`, `get <name> --entry`, `read <name> [--n] [--output]`, `delete <name>`. |
| `transformation` | `list`, `create --file|--code`. |
| `td` | `list <fv>`, `create`, `compute <fv> <ver> [--split] [--filter] [--start-time --end-time]`, `read <fv> <ver> --td-version [--split] [--output]`, `delete`. |
| `model` | `list`, `info <name> [--version]`, `register <name> <path> --framework --metrics --feature-view --td-version --input-example --schema --program`, `download <name> [--version] --output`, `delete <name> --version`. |
| `deployment` | `list`, `info <name>`, `create <model> --version --script --inference-id`, `start <name>`, `stop <name>`, `predict <name> --data`, `logs <name> [--tail]`, `delete <name>`. |
| `job` | `list`, `info <name>`, `create <name> --type --app-path --args --driver-memory --executor-cores --dynamic`, `run <name> [--wait]`, `stop <name>`, `status <name> [--wait --poll]`, `logs <name>`, `history <name>`, `delete <name>`, `schedule <name> <cron>`, `schedule-info <name>`, `unschedule <name>`. |
| `chart` | `list`, `info <id>`, `create`, `update <id>`, `delete <id>`, `generate --fg/--fv --x --y --type --dashboard`. |
| `dashboard` | `list`, `info <id>`, `create <name>`, `delete <id>`, `add-chart <id> --chart`, `remove-chart <id> --chart`. |
| `dataset` | `list [path]`, `mkdir <path>`. |
| `secret` | *(new)* `list`, `get <name>`, `create <name> <value>`, `delete <name>`. — trivial win via `hopsworks.get_secrets_api()`; not in Go. |
| `git` | *(new, optional)* `repo list/get/create`; low-priority. |
| `kafka` | *(new, optional)* `topic list/create/delete`; low-priority. |
| `env` | *(new, optional)* `python env list/create/install`; low-priority. |
| `context` | dump project state; `--json`. |
| `init` | scaffold Claude Code skill + permissions. |
| `update` | `pip install -U hopsworks` (or print instructions if installed non-editable). |
| `--version` / `-v` | reads `hopsworks_common.version.__version__`. |

Global flags: `--host`, `--api-key`, `--project`, `--json`, `-o/--output`, `--config <path>`, `--verbose`. Inherit via Click's `@click.pass_context`.

## 4. Staged delivery

Each phase is shippable on its own; gate docker-images switchover on Phase 4.

**Phase 0 — Scaffolding + `hops setup` (2 days)**
- Create `python/hopsworks/cli/` skeleton and empty stubs for every command.
- Wire `hops` / `hopsworks` console scripts in `pyproject.toml`.
- Add `click` and `tomli-w` to base deps (tomllib is stdlib on 3.11+; base requires-python is already `>=3.10`, so we either bump to 3.11 or add `tomli` too — recommend bumping since 3.10 is near EOL).
- Port `config.py` (TOML read/write, precedence chain, migration from `~/.hops/config`).
- Implement `hops setup` end-to-end against the backend token-flow endpoints (see §10).
- Implement `hops login` (flag-based fallback), `hops project {list,use,info}`, `hops --version`.
- Test: `hops setup` on a live cluster, external mode and internal mode; idempotent re-run.

**Phase 0.5 — Backend/Frontend patches (0.5–1 day, coordinated PRs in `hopsworks-ee` and `hopsworks-front`)**
The existing `modal_cli_setup` branches hardcode the API key name as `cli-<timestamp>`. User spec requires a CLI-suggested, user-editable name. Patches needed:
- **Backend** (`hopsworks-ee`):
  - Extend `TokenFlowState` with `suggestedKeyName` (set by `/create`) and `finalKeyName` (set by `/complete`).
  - Add `?key_name=<name>` query param to `POST /token-flow/create` — stored as the suggestion.
  - Add `?key_name=<name>` query param to `POST /token-flow/complete/{flowId}` — overrides the suggestion if present; otherwise uses the suggestion; final fallback to current `cli-<timestamp>`.
  - `GET /token-flow/wait/…` `WaitResponse` gains `apiKeyName` so the CLI can echo back what was persisted on the server ("Token written to ~/.hops.toml with api-key name dowling-jim").
  - Validate name: regex `[a-zA-Z0-9_-]{1,45}`, reject duplicates per user (409) so the CLI can retry with a numeric suffix.
- **Frontend** (`hopsworks-front`):
  - Extend `TokenFlow.tsx` to read the suggestion from a new `GET /token-flow/info/{flowId}` endpoint (or from `/complete`'s request body) and show an editable text input pre-filled with it.
  - `TokenFlowService.completeTokenFlow` sends `key_name` alongside `project`.
- These patches are small (~80 LoC backend, ~30 LoC frontend) but touch a different repo — keep in separate PRs, do not conflate with the CLI PR.

**Phase 1 — Reads (2 days)**
- Port `fs`, `fg list/info/preview/features`, `fv list/info`, `connector list/info`, `job list/info`, `model list/info`, `deployment list/info`, `dataset list`, `td list`.
- `output.py` with table + JSON.
- `hops context`.
- Test: every read command returns identical JSON to Go CLI against a live cluster.

**Phase 2 — Mutations via SDK (3 days)**
- `fg create`, `fg create-external`, `fg insert`, `fg derive`, `fg delete`, `fg stats --compute`, `fg search`, `fg keywords …`.
- `fv create` (incl. join-spec parser ported from Go — reuse the mini-grammar `"<fg>[:<ver>] <TYPE> <on>[=<right_on>] [prefix]"`), `fv get`, `fv read`, `fv delete`.
- `td compute`, `td read`.
- `transformation list/create` (AST parse of file-based UDFs).
- `model register`, `model download`, `model delete`.
- All in-process — delete the Go CLI's `buildPythonScript` patterns entirely.

**Phase 3 — REST escape-hatch commands (2 days)**
- `connector create/delete/test/databases/tables/preview` (REST via existing SDK client).
- `deployment create/start/stop/predict/logs/delete` (REST).
- `job run/stop/logs/history/schedule/schedule-info/unschedule` (REST through `job_api.py`).
- `chart` and `dashboard` CRUD (add thin `chart_api.py` and `dashboard_api.py` under `hopsworks_common/core/` per repo rule).
- `dataset mkdir`.

**Phase 4 — Polish + docker-images switchover (1 day)**
- `hops init` (Claude skill scaffold + permissions). Extend to also write `.claude/commands/hops.md` and `.claude/agents/hops-fti.md` (see §11).
- `hops update` (prints upgrade instructions; no self-update).
- `SKILL.md`, `hops.md` (slash cmd), and `hops-fti.md` (sub-agent) templates embedded via `importlib.resources`.
- `docs/cli/` user-facing docs page.
- **Update 3 Dockerfiles** (`terminal-server`, `terminal-gpu`, `terminal-spark`) on the `cli` branch of `docker-images`: remove the `go1.24.1` download + `git clone MagicLex/hopsworks-cli` + `go build` steps; the `hops` binary now comes for free with `pip install hopsworks` that's already in the image. This deletes ~5 lines per Dockerfile and cuts Go toolchain (~200MB) from the image.
- Smoke-test all 3 terminal images end-to-end inside a live cluster: `hops login` (internal mode), `hops project list`, `hops fg list`, `hops job run --wait`, `hops deployment predict`, `hops init`.

**Phase 5 — New commands not in Go (0.5 day, optional)**
- `hops secret list/get/create/delete` (trivial via `get_secrets_api()`).
- `hops git`, `hops kafka`, `hops env` if requested.

Total: ~8–10 engineering days. The dominant cost is Phase 2 + testing against a live cluster — everything else is mechanical.

## 5. Testing

- **Unit**: Click `CliRunner` per command, SDK calls mocked with `pytest-mock` (already in dev deps). One fixture per group that yields a mocked `Project`.
- **Config**: env-var / config-file / CLI-flag precedence matrix (~12 cases), internal-mode detection, SaaS-cache fall-through.
- **Golden-output**: table + JSON snapshot tests for read commands to catch regressions.
- **Live smoke**: a `tests/cli/live/` directory, opt-in via `HOPSWORKS_LIVE=1`, runs the full matrix against a real cluster. Not in CI by default; run manually before Dockerfile switchover.
- **docsig**: every `@public` CLI-exposed helper documented per repo rules.

## 6. Risks and mitigations

- **SDK gap**: a handful of Go CLI features use endpoints the SDK never wrapped (e.g. deployment `/logs`, connector `/data_source/data`). Mitigation: the REST escape hatch in section 2.6 + add thin `core/<entity>_api.py` modules per repo rule. No new client implementations.
- **Join-spec parser**: Go CLI has a custom mini-grammar (`"products LEFT product_id=id p_"`). Port with a single regex + tests. ~50 LoC.
- **Backwards compatibility of `~/.hops/config`**: keep the same YAML keys so existing users' configs continue to work; detect any Go-specific fields and ignore gracefully.
- **Binary name collision**: `hops` is a short name; if a user has the Go binary in a different `PATH` location, `which hops` may still resolve to the old one. `hops init` should print `which hops` and `hops --version` at end of run so the user can verify.
- **Python startup cost**: `hopsworks.login()` cold-start is ~1–2s (imports hsfs/hsml). Go is instant. Mitigate by lazy-importing `hopsworks` only when a subcommand actually needs it — `hops --version`, `hops --help`, `hops init`, `hops context --local` should not trigger SDK import.
- **Windows**: Go CLI is cross-platform. Python CLI inherits the SDK's platform matrix (Windows wheels exist but are second-class). Acceptable — the terminal images are Linux.

## 7. Open questions for you

1. Should we keep the `hops` binary name as the primary entry point, or rename to `hopsworks` and keep `hops` as an alias? (Recommend: primary `hops`, alias `hopsworks`.)
2. The `chart` + `dashboard` Go commands hit REST resources under `/project/{id}/charts` and `/dashboards` — do we actually want these in the Python SDK's `core/`, or leave them CLI-internal? (Recommend: add to `core/` per repo rule, expose as proper APIs — this is useful for non-CLI consumers too.)
3. `hops update`: pip-self-update (`pip install -U hopsworks`) is brittle in system-managed venvs like `/srv/hops/venv`. Option to just print instructions. (Recommend: detect editable/system install and print instructions; only self-update in user-owned venvs.)
4. Do we want a `/cli` slash command in this repo that spawns agents to run the phases in sequence? (Decided: no; phases are small enough for ordinary sessions.)
5. `/hops` is a separate, user-facing slash command (FTI-pattern project agent) — see §11. Distinct from the CLI implementation itself.

## 8. Deliverable for the docker-images side

Separate PR on the `cli` branch that:
- Removes Go toolchain install lines in `terminal-server/Dockerfile:152-155`, `terminal-gpu/Dockerfile` (same region, around line 175), and `terminal-spark/Dockerfile` (if present).
- Adds `pip install hopsworks[cli]` or relies on existing `hopsworks` install — new CLI ships with the base package, no extras needed unless we want to gate `rich` etc. behind an optional `[cli]` extra.
- Leaves the `hops init` call at container start intact.

Expected image-size reduction: ~200MB per image (Go toolchain removed).

## 10. `hops setup` — primary auth (Modal-style browser token flow)

Modeled on `modal setup` / `modal token new`. The backend plumbing already exists on the `modal_cli_setup` branches of `hopsworks-ee` and `hopsworks-front` (see §4 Phase 0.5 for the gaps that need patching).

### 10.1 Backend endpoints (already in `hopsworks-ee` @ `modal_cli_setup`)

Root: `{host}/hopsworks-api/api/token-flow`. All three endpoints, implemented in `TokenFlowResource.java`:

| Endpoint | Auth | Purpose |
|---|---|---|
| `POST /create?localhost_port=…&utm_source=hops-cli` | none | Server generates `flowId` + `waitSecret` + `webUrl`. Stores state in a 15-min Ehcache (no DB). Returns `{flowId, waitSecret, webUrl, code}`. |
| `GET /wait/{flowId}?wait_secret=…&timeout=40` | none (uses `waitSecret`) | Long-poll, 1s tick, up to 60s. Returns `{apiKey, workspaceUsername, timeout}` — `timeout=true` means client should retry. |
| `POST /complete/{flowId}?project=…` | JWT (browser session) | Creates API key via `apiKeyController.createNewKey(...)` with scopes `FEATURESTORE, PROJECT, JOB, DATASET_*, MODELREGISTRY, SERVING, USER`, stores it in the flow state, marks completed. |

After Phase 0.5 backend patches, `/create` and `/complete` also accept `key_name=…` (see §4 for the exact shape).

### 10.2 Client flow (`hops setup`)

1. **Resolve host.** From `--host`, then `HOPSWORKS_HOST`/`REST_ENDPOINT`, then prompt (default `https://c.app.hopsworks.ai`). Do **not** require a stored API key — that's the whole point.
2. **Short-circuit if already configured.** If `~/.hops.toml` has a valid key and `--force` is not set: call `hopsworks.login(host, api_key_value, project)` and print `Connected as <username>@<project> (key: <api_key_name>)`; exit 0. Print nothing to stdout beyond that single line so scripts can grep it.
3. **Suggest a key name.** Default suggestion: `<last>-<first>` if we can read the user's full name from a prior login cache, else `${USER}-$(hostname -s)` sanitized to `[a-z0-9_-]`. Prompt `API key name [dowling-jim]:` with editable default via `click.prompt(default=…)`. Skip the prompt if `--key-name` flag is given.
4. **Create flow.** `POST /token-flow/create?key_name=<suggested>&utm_source=hops-cli`. Parse `{flowId, waitSecret, webUrl}`.
5. **Splash.** Print something close to Modal's `token new`:
   ```
   The hops CLI needs to authenticate with Hopsworks.
   Opening your browser: https://c.app.hopsworks.ai/token-flow/tf-abc123

   If the browser doesn't open, visit the URL above manually.

   Waiting for authentication…
   ```
6. **Open browser.** `webbrowser.open(webUrl)`. With `--no-browser` (or headless detection — no `$DISPLAY`, `$SSH_CONNECTION` set), skip the open and just print the URL.
7. **Long poll.** Loop `GET /token-flow/wait/{flowId}?wait_secret=…&timeout=40`. Per-call timeout is 45s (server waits 40). Give up after `--timeout` total minutes (default 15, matching server TTL). Between calls that return `timeout=true`, immediately re-issue — no backoff (that's Modal's behavior).
8. **Persist.** On non-timeout response, write `~/.hops.toml` atomically (write to `~/.hops.toml.tmp`, `os.replace`, `chmod 0600`). Print:
   ```
   ✓ Connected to c.app.hopsworks.ai as dowling-jim
   Token written to /home/jdowling/.hops.toml with api-key name dowling-jim
   ```
9. **Verify.** Call `hopsworks.login(...)` once with the new key to confirm it works end-to-end; if it 401s, print a clear error and leave the config in place so the user can debug.

### 10.3 Splash page (frontend)

Already exists at `src/pages/user/login/tokenFlow/TokenFlow.tsx` (route `/token-flow/:flowId`) — project selector + Complete button → success state. Phase 0.5 adds an editable "API key name" field pre-filled from the CLI suggestion.

If the user is not logged in, the existing route auto-redirects to `/login?message=Please%20log%20in%20to%20complete%20CLI%20setup`, which is exactly the behavior the user asked for.

### 10.4 Internal mode (in-pod)

When `REST_ENDPOINT` and `SECRETS_DIR/token.jwt` are present, `hops setup` is a no-op:
- Reads JWT, calls `hopsworks.login()` (SDK auto-detects), prints `Connected (internal mode) as <user>@<project>`, exits 0.
- Never writes `~/.hops.toml` in internal mode — the JWT is already handled by the SDK and expires on its own.

### 10.5 Security notes

- API key printed to stdout only if `--print-key` flag is explicitly passed (scripting/CI scenario).
- `~/.hops.toml` written with mode `0600`; parent dir stays at user default.
- The `waitSecret` is the only guard against a local shoulder-surfer hitting `/wait/{flowId}`: both CLI and browser must use HTTPS, and the secret is 32-char urandom.
- The `flowId` in the browser URL is *not* secret on its own — completing requires a valid JWT.
- If the user runs `hops setup` twice in parallel, the second run creates a new flow; the first eventually expires (15 min server-side). No cleanup needed.

### 10.6 Dependencies

- **`webbrowser`** (stdlib) — `webbrowser.open()` with BROWSER env var respected.
- **`requests`** (already base dep) — for the REST calls; reuse `hopsworks_common.client` session if already initialized, else raw `requests`.
- **`tomllib`** (stdlib ≥3.11) for reading, **`tomli-w`** (new ~200 LoC dep) for writing.
- No new heavy deps (no `keyring`, no OS keychain integration — we just drop the key in `~/.hops.toml` like Modal does).

## 11. `/hops` slash command — FTI-pattern project agent

A Claude Code slash command that drives an ML project forward using Jim Dowling's **Feature / Training / Inference (FTI)** pipeline architecture. Not part of the `hops` CLI binary — it's a Claude Code asset that *uses* `hops` + the Python SDK to inspect and advance a project.

### 11.1 Goal

When a user types `/hops` in Claude Code (inside a Hopsworks terminal or any repo), the agent answers one question: **"Where is my ML system today, and what's the next concrete step?"** It does this by reading live project state from the cluster and mapping it to the FTI pipeline stages.

### 11.2 FTI stage model

Progress is tracked as a flat checklist per pipeline, each pipeline independently stageable. The agent reads state once per invocation and colours in the checklist.

```
FEATURE PIPELINE
  [ ] F0  Data source identified            — check `hops datasource list`
  [ ] F1  Feature group schema declared     — check `hops fg list`
  [ ] F2  Feature pipeline writes data      — check `hops fg preview <fg> --n 1` returns rows
  [ ] F3  Statistics computed               — check `fg stats` has results
  [ ] F4  Scheduled (or streaming)          — check `hops job schedule-info <job>`

TRAINING PIPELINE
  [ ] T0  Feature view defined              — check `hops fv list`
  [ ] T1  Training dataset materialized     — check `hops td list`
  [ ] T2  Model trained + registered        — check `hops model list` has ≥1 model
  [ ] T3  Model has evaluation metrics      — check `model info` shows metrics
  [ ] T4  Training job scheduled            — check `job schedule-info` for the trainer

INFERENCE PIPELINE
  [ ] I0  Inference target chosen           — batch (job) or online (deployment)?
  [ ] I1a Batch: scoring job exists         — check `hops job list` for inference job
  [ ] I1b Online: deployment created        — check `hops deployment list`
  [ ] I2  Predictions written back          — FG or external sink exists for predictions
  [ ] I3  Monitoring/alerts wired           — check `hops alerts list` (future)
```

The agent **never guesses** — every checkbox is driven by a specific `hops` read-only command or SDK call.

### 11.3 Where it lives

The command is delivered by `hops init`, same mechanism as the existing `SKILL.md`. Three files embedded under `python/hopsworks/cli/templates/` and written to `.claude/` on `hops init`:

- **`.claude/commands/hops.md`** — the slash command entry point. Short prompt: "Dispatch to the `hops-fti` sub-agent; it will read project state and report."
- **`.claude/agents/hops-fti.md`** — the sub-agent with the full FTI-driving system prompt, model `claude-sonnet-4-6`, tools restricted to `Read, Grep, Glob, Bash, Edit, Write`. Runs with the `Bash(hops *)` allowlist that `hops init` already grants.
- **`.claude/skills/hops/SKILL.md`** — general `hops` CLI reference (already scaffolded by Go CLI's `hops init`; port verbatim). The FTI agent reads this for command syntax.

This layering keeps the slash command trivially short while letting the sub-agent carry the stateful FTI logic.

### 11.4 Agent behavior per invocation

1. **State probe** (single pass, cheap). Issue `hops context --json` → get FGs, FVs, jobs inline. Extend with `hops model list --json`, `hops deployment list --json`, `hops td list <fv> --json` per FV, `hops job schedule-info <j> --json` per job. All read-only, all cached in the agent's working memory for the turn.
2. **Stage inference.** Apply the rules in §11.2. Output a checklist with ✓ / · / ✗ per item.
3. **Next action.** Pick the lowest-numbered unchecked item and propose one concrete next step: the code to write, which file to create, which `hops` command to run. Never propose more than one step at a time — this is a conversation, not a wizard.
4. **Offer to execute.** If the next step is a pure `hops` command (`fg create`, `job run`, `deployment start`) the agent proposes the exact invocation and waits for user confirmation. If it's code, the agent drafts the file (e.g. `feature_pipeline.py`, `train.py`, `predict.py`) in the working directory and asks the user to review.
5. **Remember nothing across invocations.** The cluster *is* the memory — no local scratchpad. Re-running `/hops` rebuilds the checklist from scratch. This avoids staleness and keeps the agent truthful.

### 11.5 FTI pattern opinions baked into the agent

The agent should nudge, not lecture. A few stance decisions:

- **Three separate pipelines, three separate files/jobs.** `feature_pipeline.py`, `training_pipeline.py`, `inference_pipeline.py`. Never combine them. This is the core FTI tenet.
- **Each pipeline writes to a canonical sink.** Feature pipeline → FG. Training pipeline → Model Registry. Inference pipeline → FG (batch) or Deployment (online). The agent nags if it sees code that short-circuits this (e.g. training directly on a DataFrame without going through a FV).
- **Online vs batch inference is a user choice, not a default.** When at stage `I0`, the agent asks. Doesn't pick for them.
- **Provenance matters.** Every derived FG gets `parents=[...]`; every model gets `feature_view` + `training_dataset_version` linkage. Agent flags missing lineage.
- **Scheduling is its own checkbox.** A pipeline that runs once in a notebook is not shipped. Agent treats `job schedule` as a first-class step, not an afterthought.

### 11.6 Relationship to existing skills

The docker-images terminal already ships a family of stage-specific skills — `hops-fg`, `hops-fv`, `hops-train`, `hops-online-inference`, `hops-batch-inference`, `hops-jobs`, `hops-dashboards`, `hops-apps`, `hops-superset`, `hops-data-sources`. These are deep-dive references for one stage each. `/hops` is the **meta-skill that orchestrates them**: it reads state, picks which stage the user is in, then directs Claude to the relevant stage-specific skill for the detail work. No duplication of content — the FTI agent's prompt is small and just routes.

### 11.7 Integration with `hops init`

Extend `hops init` (Phase 4 in §4) to additionally scaffold:
- `.claude/commands/hops.md`
- `.claude/agents/hops-fti.md`

Idempotent: don't overwrite if the files already exist and differ; print a note and continue.

### 11.8 Open questions on the agent

1. Should `/hops` also propose the FTI directory layout (scaffold `feature_pipeline.py` etc.) on first run if the working directory is empty? (Recommend: yes, behind a `--scaffold` arg; quiet by default so it doesn't litter existing projects.)
2. Should the agent write a `FTI_STATUS.md` in the repo root as an artifact, or keep status purely ephemeral (printed to chat only)? (Recommend: ephemeral. The cluster is the source of truth; a stale markdown file is worse than no file.)
3. Do we want `/hops f`, `/hops t`, `/hops i` as shortcuts to focus on a single pipeline? (Recommend: yes, lightweight; just pass the arg through to the sub-agent.)

## 9. Not doing

- No Typer, no Rich (unless we discover table-wrapping pain). Plain Click + ANSI.
- No shell completion script until a user asks — Click has built-in completion we can expose later via `hops completion bash|zsh|fish`.
- No plugin system. The CLI is ~20 commands; keep it flat.
- No telemetry. Go CLI has none either.
- No rewriting of `hopsworks_common.client` — we reuse it verbatim for REST escape hatches.
