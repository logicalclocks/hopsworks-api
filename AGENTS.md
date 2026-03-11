# Hopsworks API — AI Agent Guidelines

## Code Style

- **Python:**
  - Follow [PEP-257](https://peps.python.org/pep-0257/) for docstrings, using [Google style](https://mkdocstrings.github.io/griffe/reference/docstrings/#google-style).
  - Types and defaults are in signatures, not docstrings.
  - Use [ruff](https://docs.astral.sh/ruff/) for linting and formatting. See `python/pyproject.toml` for config.
  - Example: see [python/hsml/deployable_component.py](python/hsml/deployable_component.py).
- **Docstring best practices:**
  - Avoid tautologies; explain intent and consequences.
  - Use `Note`, `Warning`, `Danger` admonitions where needed (see [CONTRIBUTING.md](CONTRIBUTING.md) for examples).
  - One sentence per line for clear diffs.

## Architecture

- **Monorepo:**
  - Java and Python APIs live in `java/` and `python/`.
  - Python: `hopsworks/`, `hopsworks_common/`, `hsfs/`, `hsml/` are main packages.
  - Java: see `java/` for Beam, Flink, Spark modules.
- **Benchmarks:**
  - See [locust_benchmark/README.md](locust_benchmark/README.md) for benchmarking the Online Feature Store.

## Build and Test

- **Python:**
  - Install dev env: `uv sync --extra dev --all-groups --project python`
  - Lint: `uv run ruff check --fix`
  - Format: `uv run ruff format`
  - Test: `uv run pytest tests`
  - Interpreter: `uv run python`
- **CI:**
  - See `.github/workflows/python.yml` for checks (lint, format, test).

## Project Conventions

- **No types/defaults in docstrings**—always in function signatures.
- **Admonitions**: Use Google-style admonitions for important notes/warnings.
- **Public API**: All public methods/classes must have meaningful docstrings.
- **Formatting**: One sentence per line in docstrings.

## Integration Points

- **External:**
  - Hopsworks cluster (see [README.md](README.md) and [locust_benchmark/README.md](locust_benchmark/README.md)).
  - [locust](https://locust.io/) for benchmarking.
  - [ruff](https://docs.astral.sh/ruff/) for lint/format.
  - [pre-commit](https://pre-commit.com/) for git hooks.

## Security

- **API Keys:**
  - Never commit or log API keys. See [README.md](README.md) for secure usage.
- **Sensitive Operations:**
  - Docstrings must explain risks (see CONTRIBUTING.md for "Danger" admonition examples).

---

Consult [CONTRIBUTING.md](CONTRIBUTING.md) for:

- Concrete examples of Google-style docstrings and admonitions (see "Docstring Guidelines").
- How to structure and link API documentation, including patch note conventions.
- Steps for setting up development environments and running pre-commit hooks.
- Java build setup and Maven configuration.

See [README.md](README.md) for Hopsworks usage and integration details.
