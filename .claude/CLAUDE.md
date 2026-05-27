# Hopsworks API

## Commands

```bash
uv sync --extra dev --all-groups --project python # setup
uv run --project python ruff check --fix python/ # lint
uv run --project python ruff format python/ # format
uv run --project python pytest python/tests # test all
uv run --project python pytest python/tests/test_feature_group.py # test one file
uv run --project python docsig python/hopsworks python/hsfs python/hsml python/hopsworks_common # check docstrings
```

## Rules

- One sentence per line in docstrings, comments, MDs, etc.; no types or defaults in docstrings (they go in signatures)
- Every public class and method needs a docstring that explains intent, not restates the name
- HTTP calls belong in `core/<entity>_api.py`, not in domain objects
- New public entities require `@public` from `hopsworks_apigen`; deprecated ones require `@deprecated("replacement.path")`
- Annotation-only imports must live inside `if TYPE_CHECKING:` blocks
- Code using optional packages (`polars`, `confluent_kafka`, `great_expectations`, `pyarrow`) must be gated by a decorator from `hopsworks_common.decorators` (e.g. `@uses_polars`)
- `hopsworks_common` must not runtime-import `hopsworks`, `hsfs`, or `hsml`; `hsfs` and `hsml` must not runtime-import each other or `hopsworks`
- Never commit or log API keys, tokens, or credentials
- If you want to mark a section of a file with a comment, use `# region Region Name`

## Agent Docs

- @docs/architecture/README.md — package map and three-layer convention; links to packages, domain, and dependencies
- @docs/development/README.md — commands; links to setup, testing, linting, and CI
- @docs/conventions/README.md — conventions summary; links to docstring guide, code style, and public API
- @docs/caveats/README.md — known gotchas and workarounds; add new ones to this folder
