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

### Importability Check

Imports every module in the four packages and validates every `__all__` entry resolves, catching stale `from X import Y` / broken re-export shims that the test suite never exercises.

```bash
uv run --project python python python/scripts/check_importable.py
```

### PEP-8 / Public-API Check

Enforces that every function/method is either `@public`/`@deprecated` or PEP-8 private (`_`-prefixed), modulo the documented carve-outs — see @docs/conventions/public-api.md.
Flags any public-named symbol that lacks `@public` (internal code masquerading as API) and any other visibility-rule violation.

```bash
uv run --project python python python/scripts/check_pep8_public.py
```

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
- Module fails to import / `__all__` entry unresolved → fix the stale import or re-export (often a rename left a dangling `from X import Y`)
- Public-named symbol without `@public` → either add `@public` (if user-facing — confirm first) or rename it `_`-private (if internal); see the visibility decision rule

