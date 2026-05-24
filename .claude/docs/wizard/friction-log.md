# Hopsworks FTI-Wizard Walkthrough — Friction Log

End-to-end build of a next-day ETH block-count regressor (feature pipeline →
feature view → training data → train → deploy → Streamlit app). Captured the
moment each issue appeared, while the fix was still fresh.

Severity legend: 🛑 blocker · ⚠ time-waster · 🔧 papercut.

---

## 1. 🛑 `hops job deploy --cron "@daily"` rejects cron nicknames

```
Error: Schedule failed: Server response: errorCode 120001, "Nicknames not supported!"
```

`@daily`, `@hourly`, etc. are not accepted. Had to use the Quartz 6-field form
`"0 0 0 * * ?"`. The CLI help text doesn't say this — and the user-facing
prompts in the wizard literally use `@daily` in the instructions.

**Fix proposal**: translate common nicknames in the CLI before sending, or at
minimum reject them with a helpful "use a 6-field Quartz expression like
`0 0 0 * * ?`" message.

---

## 2. 🛑 Scheduler-injected `HOPS_START_TIME` breaks `fg.read()` on DATE event_time

Symptom: job ran fine via `hops job run` once, then after attaching a schedule
it started failing with:

```
HTTP 422, errorCode 120001, "Invalid date format: [2026-05-13T13:37:54Z]"
```

Root cause: the scheduler injects `HOPS_START_TIME` / `HOPS_END_TIME` as ISO
timestamps. The hsfs SDK auto-applies them to bare `fg.read()` calls. For FGs
whose `event_time` is a `DATE` column, the backend can't parse the ISO
timestamp.

Worse: `HOPS_START_TIME` was set to the schedule *registration* time
(13:37:54), which was **after** `HOPS_END_TIME=2026-05-13T00:00:00Z` — i.e.
the auto-injected window is `start > end`.

**Workaround I shipped:**

```python
import os
os.environ.pop("HOPS_START_TIME", None)
os.environ.pop("HOPS_END_TIME", None)
```

**Fix proposals**:
- Validate `start < end` before sending to the backend.
- When the FG's event_time is a date, coerce the env var to date format
  (truncate the `Tx:y:zZ` suffix).
- Document the env-var fallback prominently in the SDK docstring for
  `fg.read()` — the first time I read the code I had no idea this magic existed.

---

## 3. ⚠ FG description silently capped at 256 chars

```
HTTP 400, errorCode 270092, "description ... is too long with 312 characters.
Entity descriptions cannot be longer than 256 characters."
```

The limit isn't in the SDK signature or docstring. Discovered only at insert
time, after the whole pipeline had already done its work.

**Fix proposal**: validate length client-side at `get_or_create_feature_group`
construction, not at first insert.

---

## 4. 🛑 `online_enabled=False` silently breaks downstream deployment

The hops-fg skill says "default to offline unless the user needs online
serving." I followed that. Steps 2–5 worked. Step 7 (`model.deploy()` →
predictor `fv.get_feature_vector()` lookup) needed RonDB and there is **no
warning** when you attach an offline-only FG to an FV that will later be used
for serving.

I burned ~10 minutes building, sanity-checking, and deleting the offline FG
before I realised. The user explicitly called it out ("pourquoi delta ffs").

**Fix proposals**:
- When `fv.init_serving()` is called against an FV whose query touches an
  offline-only FG, raise a clear error at that point, not silently fall through.
- Even better: at `create_feature_view` time, if any FG in the query is
  offline-only, log a warning "this FV will not support `get_feature_vector`."

---

## 5. ⚠ Cross-source FG sparsity produces a degenerate label

Public shared FGs in `hopsworks_default` have wildly different histories:

| FG                       | rows |
|--------------------------|------|
| eth_blocks_daily         | 365  |
| hacker_news_btc_daily    | 90   |
| btc_blocks_daily         | 30   |
| gdelt_events_daily       | 30   |
| btc_transactions_daily   | **7** |
| wikipedia_btc_daily      | 7    |
| eth_transactions_daily   | 7    |

Outer-joining on a contiguous daily grid + forward-fill + zero-fill produces a
joined table that *looks* full (`completeness=1.0` on every column in
`hops fg stats`) but the label distribution is collapsed: 357 of 364 rows of
`btc_tx_count` are zero-imputed.

The model trained on this would happily learn "predict 0". I caught it in the
stats pass and switched targets (BTC tx_count_next_day → ETH
block_count_next_day) but only because I went looking.

**Fix proposals**:
- `hops fg list` and/or `hops fg info` should surface `row_count` so coverage
  mismatches are obvious before joining.
- The hops-reqs / hops-feature-pipeline skill should include a "check label
  coverage" step before committing to a prediction problem.

---

## 6. 🛑 `get_train_test_split(primary_key=False, event_time=False)` does not drop PK

```python
X_train, X_test, y_train, y_test = fv.get_train_test_split(
    training_dataset_version=1,
    primary_key=False,
    event_time=False,
)
X_train.astype("float32")
# TypeError: float() argument must be a string or a real number, not 'datetime.date'
```

`day` (the PK *and* event_time) was still in the dataframe despite both flags
being `False`. Probably because the materialised TD parquet file contains the
column regardless and the flags only filter "future" reads.

I had to manually `df.drop(columns=["day"])` in `train.py`. Two failed
executions before I figured out which flag wasn't doing what its name says.

**Fix proposal**: when both flags are false, drop the column server-side
post-read so the flag's name matches its behaviour. Or rename the flag to
`include_primary_key_when_materialising_new_td=...`.

---

## 7. 🛑 `from hsfs import udf` is documented but doesn't exist

```
Error: UDF import failed: cannot import name 'udf' from 'hsfs'
```

Both the hops-fv skill and the hsfs docstrings include `from hsfs import udf`.
The actual import is `from hsfs.hopsworks_udf import udf`. Browsing the source
made it obvious; the skill docs and inline examples did not.

**Fix proposal**: either re-export `udf` from the `hsfs` package
(`hsfs/__init__.py`) or fix the docs/skills uniformly. The half-and-half state
is the worst case.

---

## 8. 🔧 Deployment name regex is alphanumeric-only

```
HTTP 422, "Serving name must follow regex: \"[a-zA-Z0-9]+\""
```

Hyphens (matching my preferred kebab-case `eth-next-day-blocks`) are rejected.
Had to use `ethnextdayblocks`. The CLI help text doesn't mention this.

**Fix proposal**: surface the regex in `--name` help text, or accept `-`/`_`
and slugify server-side.

---

## 9. 🛑 `hops deployment create --script` doesn't upload, requires HDFS path

```
HTTP 400, errorCode 240016, "Predictor script does not exist"
```

…even though `--script /hopsfs/Users/meb10000/predictor.py` points at a file
that exists at that mounted HDFS path. The CLI passes the local path
verbatim, and the backend doesn't see it.

`hops job deploy` does auto-upload. `hops deployment create` does not.
Inconsistent.

Workaround I had to write a separate `deploy_model.py`:

```python
ds.upload(PREDICTOR_LOCAL, "Resources/ethnextdayblocks", overwrite=True)
model.deploy(script_file=f"/Projects/{project.name}/Resources/.../predictor.py")
```

**Fix proposal**: `hops deployment create --script <localpath>` should mirror
`hops job deploy <localpath>` — upload to `Resources/<dep_name>/` and pass the
HDFS path under the hood.

---

## 10. 🛑 Model files not copied to `Deployments/<name>/<v>/`

After `model.save(dir)`:
- Artifacts land at `/hopsfs/Models/<name>/<v>/Files/{model.json, …}`.
- `Deployments/<name>/<v>/` contains only `predictor-predictor.py`.
- The serving container mounts `Deployments/<name>/<v>/` to `/mnt/artifacts`.

Result: the predictor's `os.path.join(ARTIFACT_FILES_PATH, "model.json")`
fails because `model.json` isn't there. The container repeatedly crashloops
with an `XGBoostError: No such file or directory`.

Manual workaround: `cp /hopsfs/Models/.../Files/{model.json,feature_names.json}
   /hopsfs/Deployments/.../`. Then the predictor finds them.

**Fix proposal**: at deployment-create time, copy the contents of
`Models/<name>/<v>/Files/` into `Deployments/<dep>/<v>/`, OR mount both into
`/mnt/artifacts`, OR expose a `MODEL_FILES_PATH` env var that points at the
model files dir.

The hops-online-inference skill examples all use
`os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl"` — so they're hitting (or
papering over) the same problem.

---

## 11. ⚠ Predictor edits don't take effect on `hops deployment start` alone

After editing `Deployments/<name>/<v>/predictor-predictor.py` in place and
running `hops deployment start <name>`, the container started with the *old*
predictor. Had to do `stop` then `start` for the change to take.

**Fix proposal**: `start` after a failed run should always pull a fresh image
+ script. Or document this requirement.

---

## 12. ⚠ CLI `--data` validator rejects dict-style instances

```
$ hops deployment predict ethnextdayblocks --data '{"instances": [{"day": "2026-05-05"}]}'
Error: Predict failed: Instances field should contain a 2-dim list.
```

The validator in `serving_engine._validate_inference_data` enforces 2-dim
numerical lists. But the SDK `deployment.predict(inputs=[{"day":...}])` path
(`_validate_inference_inputs`) does accept dicts — and the server-side
predictor happily handles them.

So the CLI is artificially stricter than both the SDK and the server.

**Fix proposal**: allow dict-shaped items in `--data instances` when the
predictor is a custom Python predictor.

---

## 13. 🛑 `get_feature_vectors` batch silently returns 0 rows for ISO-date strings

The most painful issue. Same primary key, two outcomes:

```python
fv.get_feature_vector(entry={"day": "2026-05-05"})        # shape (1, 70)  ✓
fv.get_feature_vectors(entry=[{"day": "2026-05-05"}])     # shape (0, 70)  ✗
fv.get_feature_vectors(entry=[{"day": datetime.date(2026,5,5)}])  # shape (1, 70)  ✓
```

The plural form rejects ISO-date strings without raising — it just returns an
empty DataFrame. The predictor then returns `{"predictions": []}` with no
error. Silent failure → wasted ~10 minutes diffing my code before I noticed
the call was returning empty.

**Fix proposals**:
- Make the singular and plural forms accept the same input formats.
- If a PK type mismatch causes a 0-row response in batch lookup, log a warning
  ("entry `day=2026-05-05 (str)` did not match column type `date`; returning
  empty").

---

## 14. 🔧 `hops deployment status` doesn't exist

The wizard instructions used `hops deployment status <name> until ready`. The
actual command is `hops deployment info <name>`. Minor — just a doc mismatch.

---

## 16. 🛑 `hops app create --path` rejects absolute HopsFS paths

```
$ hops app create ethblocksforecast --path /hopsfs/Users/meb10000/app.py --start
HTTP 400, errorCode 130047, "The configured application file
hdfs:///Projects/onboarding_project_test//hopsfs/Users/meb10000/app.py does not exist."
```

The CLI naively concatenates the project root with the value of `--path`,
which produces a doubled-`//` HDFS URL when the user passes the mounted
absolute path. Required path is project-relative
(`Users/meb10000/app.py`).

This is the opposite of how `hops job deploy <path>` works (which accepts the
local/absolute path and auto-uploads). Two adjacent CLIs disagree on path
semantics.

**Fix proposal**: pick one — either auto-strip the `/hopsfs/` mount prefix in
`hops app create`, or have it upload from a local path the way
`hops job deploy` does.

---

## 17. 🔧 `hops app start --wait` doesn't exist; only `--no-wait` does

The wizard instructions used `hops deployment start <name>` (deployment, which
has `--wait`) and I assumed apps shared the flag. `hops app start` accepts
`--no-wait` only — `start` blocks by default. Two-CLI consistency would help.

---

## 19. 🛑 `deployment.predict(inputs=...)` blows up on `datetime.date` instances

```
TypeError: Object of type date is not JSON serializable
  File ".../hsml/core/serving_api.py", line 284, in _send_inference_request_via_rest_protocol
    data=json.dumps(data),
```

The SDK does a bare `json.dumps(data)` on the inference payload — no
`default=str` fallback, no datetime handler. So if you do the natural thing in
a Streamlit app:

```python
deployment.predict(inputs=[{"day": latest["day"].date()}])  # KABOOM
```

…you get a confusing `TypeError` that points at the SDK internals, not at your
input.

Combined with #13 (`get_feature_vectors` requires `datetime.date` for batch
lookup on the server side), there's a real cliff: the SDK forces you to send a
string, then the FV silently returns 0 rows for that string unless the
predictor converts it back to a date. The predictor I wrote has a `_to_date`
shim exactly to bridge this gap.

**Fix proposal**: use `json.dumps(data, default=str)` in
`_send_inference_request_via_rest_protocol`, or make `get_feature_vectors`
accept ISO date strings. Either one removes the cliff.

---

## 18. 🔧 `python-app-pipeline` env lacks `plotly`

The default Streamlit-friendly env doesn't include `plotly`. Surprising for a
"build interactive apps" environment. The runtime failure surfaces as
`ModuleNotFoundError: No module named 'plotly'` inside the app log, which
means the user has already paid for the upload + start cycle before noticing.

**Fix proposal**: either (a) include `plotly` in `python-app-pipeline` (it's
the default for a reason), or (b) make `hops env list` show installed packages
so the user can pick a compatible env, or (c) have `hops app create` lint
imports against the env's package list before uploading.

(Altair is bundled with Streamlit and worked fine — but the user shouldn't
have to discover that via a crash.)

---

## 15. 🔧 hops-online-inference skill examples are sklearn-pkl-centric

Every example uses
`joblib.load(os.environ["ARTIFACT_FILES_PATH"] + "/model.pkl")`. For an
XGBoost model saved as `model.json` (the recommended XGBoost format), this
doesn't work and there's no example to crib from.

**Fix proposal**: include at least one non-joblib example
(`xgb.XGBRegressor().load_model(...)`, `tf.keras.models.load_model(...)`).

---

## Net summary

Hops did its job — every artifact is real and the FTI flow is genuinely
end-to-end usable — but the path is full of small surprises that cost more
time than the actual ML work. The two highest-leverage fixes (by my
stopwatch):

1. **#10 Model files into `Deployments/`** — the single biggest blocker, cost
   ~15 min and would have stopped a less-stubborn user entirely.
2. **#13 Silent empty batch lookup on string dates** — same shape, totally
   different cost; this one *will* burn anyone debugging "why are my
   predictions empty?".

Everything else is tractable as docs / CLI polish.
