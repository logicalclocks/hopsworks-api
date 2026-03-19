# Conventions

## Summary

Docstrings follow Google style: one sentence per line, no types or defaults (those go in signatures), every public entity documented with intent not just name, `Warning:`/`Danger:` admonitions must explain consequences not just flag risk.

Code style is enforced by Ruff (line length 88, double quotes, Python 3.9+).
Annotation-only imports go in `if TYPE_CHECKING:` blocks.
Logger is always `_logger = logging.getLogger(__name__)`.

Public API is marked with `@public`, deprecated items with `@deprecated("replacement.path")`.
Cross-references in docstrings use `` [`ClassName`][full.module.path.ClassName] `` syntax.

## Details

- @.agent/conventions/docstrings.md — structure, tautology examples, admonitions, deprecation, patch notes, linking
- @.agent/conventions/code-style.md — naming, imports, type hints, error handling, logging, serialization
- @.agent/conventions/public-api.md — `@public`, `@also_available_as`, `@deprecated`, doc pages, patch notes
