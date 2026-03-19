# CI

Defined in `.github/workflows/python.yml`.
All jobs must pass before a PR can be merged.

## Jobs

### Lint and Style Check

Runs first; test jobs depend on it.

```bash
uv run --project python ruff check python/
uv run --project python ruff format --check python/
uv run --project python docsig python/hopsworks python/hsfs python/hsml python/hopsworks_common
```

Fails if any linting error is present, if formatting differs from what `ruff format` would produce, or if any docstring `Parameters:` section mismatches its signature.

### Unit Tests — Main Matrix

Python versions: 3.9, 3.10, 3.11, 3.12, 3.13.
Full dev install.
`PYSPARK_SUBMIT_ARGS` set to include the Avro package.
Timezone set to `UTC`.

### Unit Tests — No Optional Dependencies

Python 3.10.
Core install only, no extras.
Ensures optional-dep code paths do not accidentally import optional packages at module load time.

### Unit Tests — Pandas 1.x

Python 3.9–3.11.
Installs `pandas<2.0`.
Verifies backwards compatibility.

## What Makes Each Job Fail

- Formatting differs → run `ruff format`
- Lint rule violation → run `ruff check --fix`, fix remaining manually
- Docstring/signature mismatch → align `Parameters:` with signature
- Test assertion fails → fix the logic or update the test
- Import error at test collection → check for missing guards on optional imports

