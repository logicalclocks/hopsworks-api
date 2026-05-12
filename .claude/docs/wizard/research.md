# Wizard Brief: Autoresearch

You are running an autonomous research loop on Hopsworks data. The wizard already opened a Terminal and dropped you in. Your job: a short conversation with the user, then go autonomous until the budget is exhausted.

## What the wizard handed you

- `features`: list of `fg_v.feature` references the user picked, possibly empty
- `intent`: plain-English target ("minimize MAE on next-day txs_per_block")
- `budget`: N experiments at ~5 minutes each (default 10)
- `compute`: `cpu` or `gpu`
- `tag`: short run slug; default to today's date if missing

Any of these may be empty. Ask once, in plain English, only the ones you really need. Don't quiz the user.

## Tools you must use

Everything outside `train.py` goes through `hops`. `train.py` itself logs into the SDK (it has to load data and fit a model in-process).

- `hops context`: what the project currently has
- `hops fg list / info / features / preview / create / insert`: feature groups
- `hops fv list / info / create / read`: feature views
- `hops model register / info / list / delete`: model registry
- `hops job deploy`: if the user wants the training to recur after the run

Do **not** open a python heredoc to call `mr.python.create_model(...)` or `fg.insert(...)`. That breaks repeatability for the next agent picking this up. If a CLI verb is missing for what you need, fix the CLI in passing instead of working around it.

## Setup (one conversation, then go)

1. `hops context`: show the user what's already there.
2. If `features` is empty, run `hops fg list` and `hops fg features <fg> --version <v>` on candidates, then ask which to use.
3. Create the feature view:
   ```
   hops fv create autoresearch_<tag>_fv --feature-group <base>:<v> --labels <label>
   ```
4. Create the experiments log feature group:
   ```
   hops fg create autoresearch_experiments_<tag> \
     --primary-key commit --event-time ts \
     --features "commit:string,val_metric:double,peak_memory_gb:double,status:string,description:string,ts:timestamp"
   ```
5. Write `train.py` (single file, single process) that:
   - logs into Hopsworks (`import hopsworks; hopsworks.login()`)
   - reads the FV (`fv.query.read()`)
   - fits a baseline appropriate to the problem (regression: XGBRegressor; classification: XGBClassifier; tiny data: consider Ridge / LogisticRegression)
   - saves artefacts under `model/`
   - prints exactly these three lines last:
     ```
     val_metric: <float>
     peak_memory_gb: <float>
     training_seconds: <float>
     ```
   - honours a 5-minute wall-clock budget and prints the three lines even on timeout
6. Commit, run, log baseline row, register baseline model:
   ```
   echo '[{"commit":"<sha>","val_metric":<v>,"peak_memory_gb":<m>,"status":"keep","description":"baseline","ts":"<iso>"}]' > /tmp/row.json
   hops fg insert autoresearch_experiments_<tag> --file /tmp/row.json
   hops model register autoresearch_<tag> model/ \
     --framework python --metrics "val_metric=<v>" \
     --description "baseline; status=keep; metric_direction=<min|max>; commit=<sha>" \
     --feature-view autoresearch_<tag>_fv
   ```
7. Show baseline number to the user. Confirm once. Then go autonomous.

## The loop (no further confirmations)

```
while exp < budget:
    1. edit train.py with ONE idea (commit message says what)
    2. git commit -am "exp: <change>"
    3. python train.py > run.log 2>&1
    4. parse: val_metric / peak_memory_gb / training_seconds
       (empty output means crash; tail run.log, attempt fix; few tries max)
    5. write /tmp/row.json with status=keep|discard|crash, hops fg insert
    6. hops model register autoresearch_<tag> model/ ...
       (every exp registers, so the registry UI shows the run as one
       model with N versions, regardless of keep/discard)
    7. if val_metric improved per metric direction:
         - keep the commit; status=keep
       else:
         - git reset --hard HEAD~1; status=discard
```

Never ask "should I keep going?". The user might be asleep.

## Conversation rules

- One question per turn. Bundling four questions into one paragraph kills the back-and-forth.
- After each segment (setup / each exp), summarise in one line. Don't recap the loop every time.
- If the user pushes back on a direction mid-run, stop the loop, integrate, then resume.

## Anti-patterns

- Heredoc Python for any orchestration step (insert, register, delete). Use the CLI.
- Skipping the experiments FG row on crashes. Every attempt is a row, status=crash.
- Tuning multiple knobs in one commit. The leaderboard becomes meaningless.
- Redefining the metric mid-run. `intent` pins it.

## See also

- `hopsworks-autoresearch/program.md`: the loop contract this brief implements
- `docs/onboarding-flow.md`: the broader 10-step wizard reference
- `docs/cli/`: `hops` subcommand reference if you're unsure of a verb
- `docs/caveats/`: known SDK / CLI gotchas
