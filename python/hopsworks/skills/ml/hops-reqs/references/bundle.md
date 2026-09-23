# Run bundles and results

`hops job deploy` uploads one file, the entrypoint. Everything else a run needs
(the `system.yaml` it reads its budget or window from, the `evaluate.py` it
imports, the tests it runs) travels in one immutable **bundle** per run, and
everything it measured comes back in one **result file** per run. No job writes
`system.yaml`: a file written inside the cluster does not reach the repository.

## The bundle

`bundle.py` in the system directory builds it from the committed files and
refuses to include uncommitted changes, so a run is reproducible from its commit:

```bash
python <slug>/bundle.py make train-3-1                     # system.yaml, pyproject.toml, src/
python <slug>/bundle.py make tests-features-1 --with-tests # plus tests/ and benchmarks/
hops files upload <slug>/runs/train-3-1/bundle.tar.gz Resources/<slug>/runs/train-3-1/
```

It contains `manifest.json`:

```json
{"run_id": "train-3-1", "commit": "e04c7d2...", "slug": "telco-churn",
 "files": {"system.yaml": "<sha256>", "pyproject.toml": "<sha256>", "src/telco_churn/evaluate.py": "<sha256>"}}
```

A bundle is never rewritten; a new run gets a new id (`<kind>-<n>-<attempt>`,
for example `train-3-2` for the second attempt of run 3). `runs/` is gitignored.

## The prelude

Every entrypoint starts with the same region, `# region bundle prelude`, copied
unchanged from the template. `_load_bundle(--bundle)` downloads the archive when
it is a HopsFS path, extracts it, refuses it when its files differ from the
manifest or a hash does not match, puts the bundle's `src/` first on
`sys.path`, and loads `system.yaml`. The training program then imports
`evaluate` from the bundle, so the harness a run scores with is the one its
commit names:

```python
_, manifest, system = _load_bundle(args.bundle)
evaluate = importlib.import_module(f"{system['system']['slug'].replace('-', '_')}.evaluate")
```

A scheduled job is deployed with the bundle of the run that deployed it in its
`--args`, so every fire runs the code of that commit until a new deploy.

## The result

`_write_result(manifest, {...})` writes `Resources/<slug>/runs/<run_id>/result.json`
with the run id and commit echoed from the manifest, plus what the program
measured: metrics, the registry model name and version registration returned,
rows written, benchmark percentiles and counts. With `HOPS_RESULT_DIR` set it
writes locally instead, which is how the unit tests exercise it.

The orchestrator imports it:

```bash
hops files download Resources/<slug>/runs/train-3-1/result.json --output /tmp/train-3-1.json --overwrite
```

It checks the run id and commit against the row it wrote before submitting
(`state: submitted`), adds the execution id, and writes the row into the YAML
with `state: finished` (or `failed`). The metrics for a research run are then
read back from `hops model info <ident>_research --version <v>` with the version
the result names. `--record` on a benchmark means "write result.json", nothing more.

## Intent before effect

1. Commit the code (`[<slug>] train run <n>: <what changed>`).
2. Build and upload the bundle.
3. Write the row with `state: submitted`, its `run_id`, `commit` and `bundle`.
4. Submit (`hops job deploy ... --run`), record the execution id, `state: running`.
5. Poll `hops job history <job>`; on completion download `result.json`, complete the row.
6. Commit the evidence (`[<slug>] train run <n>: result`).

A session that dies between 3 and 5 leaves a row a resume reconciles against
`hops job history`, instead of a duplicate job.
