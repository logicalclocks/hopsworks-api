# Setup

## Python

Prerequisites: Python 3.9–3.13, `uv` (recommended) or pip.

```bash
uv sync --extra dev --all-groups --project python
```

Creates `python/.venv/` and installs all four packages in editable mode with all development dependencies (pytest, ruff, docsig, pyspark, moto, delta-spark).

To install specific extras only:

```bash
uv sync --extra python --extra great-expectations --project python
```

To run commands without activating the venv:

```bash
uv run --project python pytest python/tests
uv run --project python python my_script.py
```

### Pre-commit

```bash
pre-commit install
```

Runs Ruff on every commit.
If it modifies files, the commit aborts — stage the fixes and commit again.
Does not run docsig; run that manually before pushing.

## IDE

Install the Ruff extension and configure format-on-save for Python.
Recommended VS Code settings:

```json
{
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true
  }
}
```
