# Testing

## Structure

Tests live in `python/tests/` and mirror the source structure.

- `conftest.py` — global fixtures, `PYSPARK_SUBMIT_ARGS`, environment setup
- `fixtures/` — JSON payloads for round-trip tests and Python fixture functions
- `test_*.py` — one file per major domain class
- `core/` — tests for `core/` API and engine modules
- `engine/` — tests for execution engine modules
- `hopsworks/` — tests for the `hopsworks` package
- `utils/` — shared test helpers

## What Tests Cover

Unit tests exercise in-process logic only: serialization, deserialization, query building, filter construction, validation, and engine logic.
No network requests, no real Hopsworks cluster.
Integration tests are in a separate private repository.

## Writing New Tests

- Mirror the source: tests for `hsfs/feature_group.py` go in `tests/test_feature_group.py`
- Test round-trip serialization for every new domain object: `obj.to_dict()` → `Class.from_response_json(dict)`
- Test edge cases explicitly: missing optional fields, `None` values, empty lists
- When testing an API module, mock only the HTTP client — not the domain objects
- Do not mock the full domain layer to test domain logic

## Running Tests

```bash
uv run --project python pytest python/tests # all tests
uv run --project python pytest python/tests/test_feature_group.py # one file
uv run --project python pytest python/tests -k "feature_view" # by keyword
uv run --project python pytest python/tests -x # stop on first failure
uv run --project python pytest python/tests -v # verbose
```

## CI Test Matrix

Three test runs in CI (see @docs/development/ci.md):

- Main matrix: Python 3.9–3.13, full dev install
- No optional deps: Python 3.10, core install only
- Pandas 1.x: Python 3.9–3.11, `pandas<2.0`
