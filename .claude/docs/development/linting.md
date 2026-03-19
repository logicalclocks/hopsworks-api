# Linting and Formatting

## Tools

- `uv run --project python ruff check --fix python/` — linting with auto-fix where possible
- `uv run --project python ruff format python/` — formatting (replaces Black)
- `uv run --project python docsig python/hopsworks python/hsfs python/hsml python/hopsworks_common` — docstring/signature consistency check
- `pre-commit install` (once) then `git commit` — runs Ruff automatically on every commit

All Ruff configuration is in `python/pyproject.toml` under `[tool.ruff]`.

## Ruff Settings

- Line length: 88
- Quote style: double
- Target: Python 3.9+
- `E501` (line too long) is ignored — the formatter handles line length

Active rule groups:

- `E`, `W` — pycodestyle: whitespace, blank lines, statement style
- `F` — Pyflakes: undefined names, unused imports
- `B` — flake8-bugbear: likely bugs and bad practices
- `I` — isort: import ordering
- `D2`–`D4` — pydocstyle: docstring presence and format
- `UP` — pyupgrade: outdated syntax
- `TC` — flake8-type-checking: `TYPE_CHECKING` block discipline
- `RET` — flake8-return: redundant return statements
- `SIM` — flake8-simplify: simplifiable constructs

## Docsig

Verifies that docstring `Parameters:` sections match function signatures.
Catches: parameters documented but not in signature, signature parameters missing from docstring, type information in docstrings.

```bash
uv run --project python docsig python/hopsworks python/hsfs python/hsml python/hopsworks_common
```

## Common Failures

- `D401` — use imperative mood: "Return a list…" not "Returns a list…"
- `D205` — one blank line required between summary line and description
- `D400` — summary line must end with `.`, `?`, or `!`
- `TC001`/`TC002` — move annotation-only imports into `if TYPE_CHECKING:`
- `B007` — rename unused loop variable to `_`
- docsig mismatch — align `Parameters:` names and order with the signature exactly
