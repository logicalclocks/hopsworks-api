# Docstrings

All public API docstrings follow Google style and are rendered by mkdocs-material into the API reference.

## Core Rules

- One sentence per line — each sentence ends with `.`, `?`, or `!` and occupies its own line
- No types or defaults — they are extracted automatically from the signature
- No tautologies — provide information the user cannot derive from the name alone
- Explain *why*, not just *what* — especially for warnings, dangers, and non-obvious behavior
- Proper sentences — start with a capital letter, end with punctuation (exception: admonition titles)

## Structure

```python
def method(self, param1: str, param2: int = 0) -> list[str]:
    """Summary line in imperative mood, ending with a period.

    Optional extended description.
    One sentence per line.

    Note: Note Title
        Body of the note.

    Parameters:
        param1: What it is and how it is used.
        param2:
            Multi-line description when one line is not enough.
            Second line indented, starting on the line after the name.

    Returns:
        What is returned and under what conditions.

    Raises:
        SomeError: When and why this error is raised.

    Example: A short title for the example
        ```python
        result = obj.method("value", param2=5)
        ```
    """
```

- Use `Yields` instead of `Returns` for generator functions
- Use `Example:` admonition (singular) — `Examples:` section is not rendered correctly

## Tautologies

Bad — restates the name:

```python
"""Description of the feature."""
```

Good — adds new information:

```python
"""The description shown in the Hopsworks UI when browsing features."""
```

## Admonitions

```
Note: Title
    Body text.

Warning: Title
    Body text.

Danger: Title
    Body text.

Info: Title
    Body text.
```

`Warning:` and `Danger:` must explain the concrete consequences, not just flag existence of risk.

Bad:

```
Danger: Potentially dangerous operation
    This operation stops the execution.
```

Good:

```
Danger: Kills the job without graceful shutdown
    This operation sends SIGKILL to the job process, bypassing shutdown hooks.
    In-flight writes may be left in a partially committed state, which can corrupt
    the offline feature store and require manual recovery.
```

## Deprecation

Deprecating a parameter:

```python
"""Does something.

Warning: Deprecated Parameters
    Parameter `old_param` is deprecated and will be removed in Hopsworks v5.0.
    Use `new_param` instead.

Parameters:
    old_param: Deprecated, use `new_param` instead.
    new_param: The replacement parameter.
"""
```

Deprecating a cross-namespace alias:

```python
"""...

Warning: Deprecated Aliases
    Alias [`hsfs.function`][hsfs.function] is deprecated and will be removed in a future release.
"""
```

Always specify the version as `major.minor`.

## Patch Notes

When adding behavior in a patch release, add an `Info:` admonition with a `~=X.Y.Z` title:

```python
"""Does something.

Info: Accepts polars DataFrames, ~=4.8.1
    Starting from version 4.8.1, this method accepts polars DataFrames in addition
    to pandas DataFrames.
"""
```

## Linking

```markdown
[`FeatureGroup`][hsfs.feature_group.FeatureGroup]
[`FeatureGroup.insert`][hsfs.feature_group.FeatureGroup.insert]
```

Always use the full module path in brackets.
Include the class name when linking methods.

External links use plain Markdown syntax:

```markdown
[Hopsworks documentation](https://docs.hopsworks.ai)
```
