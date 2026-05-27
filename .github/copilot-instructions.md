# GitHub Copilot Review Instructions

## Code Structure

- Flag HTTP calls (`requests`, `client._send_request`, ...) placed directly in domain object methods (`feature_group.py`, `feature_view.py`, `model.py`, ...).
  HTTP calls belong in `core/<entity>_api.py` modules.
- Flag optional package imports (`polars`, `great_expectations`, `confluent_kafka`, `pyarrow`, ...) at module level.
  They must be inside `if TYPE_CHECKING:` blocks or inside functions guarded by an appropriate decorator (e.g., `@uses_polars`).
- Flag runtime imports of `hsfs` or `hsml` from `hopsworks_common`, runtime imports of `hopsworks` from `hsfs` or `hsml`, and runtime imports between `hsfs` and `hsml`.
  Imports inside `if TYPE_CHECKING:` blocks are fine.

## Public API

- Flag methods and classes meant to be in the user-facing public API introduced without `@public` from `hopsworks_apigen`.
- Flag deprecated methods that do not use `@deprecated("replacement.path")`, if it can be used instead of `Warning:` admonition.

## Error Handling

- Flag unclear error messages without concrete details.

## Security

- Flag any code that logs, prints, or stores API keys, tokens, or credentials.
- Flag hardcoded credentials, tokens, or secret values of any kind.
- Flag SQL or shell strings constructed by plain string concatenation with user-provided values — use parameterized queries or `shlex` escaping.

## Docstrings

- Flag any public class, method, or function that lacks a docstring.
- Flag docstrings that restate the function name without adding information (tautologies).
  Example: `"""Delete the feature group."""` on a `delete()` method is insufficient — it must explain what is irreversible, what data is removed, and what the consequences are.
- Flag `Warning:` or `Danger:` admonitions that do not explain *why* an operation is risky and what the concrete consequences are.
- Flag type or default value information placed in docstrings — these belong in the function signature only.
- Flag docstrings where multiple sentences are on the same line.
  Each sentence must be on its own line.
  Same applies to comments, markdown files, and any other prose.
- Flag `Examples:` sections — the correct form is the `Example:` admonition.

## Type Hints

- Flag public functions missing return or parameters type annotations.
- Suggest type hints for functions missing them, if you can figure out the types.

## Tests

- Flag new domain logic without a corresponding test in `python/tests/`.
- Flag tests without assertions.
- Flag tests that assert exact opaque return values without explaining what property is being verified — a test should read like an example that makes the intent obvious.
- Prefer tests that verify meaningful properties (output structure, type, invariants, edge case behavior) over tests that merely assert `f(a) == b` with no evident rationale.
- Flag test names or bodies that do not make clear what behavior is under test and why it matters.

## Minor

- Flag typos.
- Flag leftover `print()` statements or commented-out code.

## General Review Practices

- Raise concerns about logic correctness before style concerns.
- When flagging an issue, briefly state what the problem is, why it matters, and what the fix can be.
