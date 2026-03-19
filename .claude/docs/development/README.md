# Development

## Commands

```bash
uv sync --extra dev --all-groups --project python # setup
uv run --project python ruff check --fix python/ # lint
uv run --project python ruff format python/ # format
uv run --project python pytest python/tests # test all
uv run --project python pytest python/tests/test_feature_group.py # single file
uv run --project python pytest python/tests/test_feature_group.py::TestClass::test_method # single test
uv run --project python pytest python/tests -k "feature_view" # by keyword
```

## Details

- @docs/development/setup.md — environment setup, pre-commit, IDE config
- @docs/development/testing.md — test structure, fixtures, what to test and how
- @docs/development/linting.md — Ruff rule groups, docsig, common failures and fixes
- @docs/development/ci.md — CI jobs and what triggers each failure
