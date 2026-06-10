# Public API

## Every symbol is either public or private

Each function and method must be exactly one of:

- **Public** — user-facing API, annotated `@public` (or `@deprecated` for a retiring public symbol) and documented.
- **Private** — internal, named with a leading underscore (`_foo`); PEP-8 signals "do not depend on this".

There is no third state.
A public-named symbol (no leading underscore) that lacks `@public`/`@deprecated` is a defect: it is internal code masquerading as API.
This is enforced in CI by `python/scripts/check_pep8_public.py` (the `pep8_public` job) — see @docs/development/ci.md.

### Deciding visibility — and confirming it

For **every new or moved symbol**, and before changing any symbol's visibility, make a deliberate public-vs-private decision rather than defaulting:

1. Ask: is this part of the surface a user is meant to call, or internal plumbing?
2. Check the evidence before deciding it is public — a symbol is public only if at least one holds:
   - it is used by a real consumer (e.g. the `loadtest` repo) **as our API** (not re-imported from a third-party package);
   - it appears in the published docs (`logicalclocks.github.io`);
   - its docstring/signature clearly describes user-facing intent.
3. If internal, give it a leading underscore. If public, add `@public(...)` with a full docstring.

**Get explicit confirmation before changing a symbol's visibility.**
Do not unilaterally promote a symbol to `@public`, and do not privatize a symbol that might be user-facing, on your own judgment.
When the right call is uncertain (it looks user-facing, is used in `loadtest`, or appears in the docs), surface the specific symbol and your reasoning and ask — never guess.
Renaming a public symbol to private (or vice versa) is a breaking API change; it needs a human sign-off, not a silent edit.

### Carve-outs (stay public-named without `@public`)

These are not violations and must not be underscored:

- `@property` accessors and their setters/deleters.
- Serialization hooks called by name: `json`, `to_dict`, `to_json`, `to_json_dict`, `from_response_json`, `from_response_json_single`, `from_response_json_list`, `from_json`, `from_dict`, `update_from_response_json`, `extract_fields_from_json`, `from_string`, `from_user_input`.
- Dunders (`__init__`, `__enter__`, …).
- A method that overrides a `@public` abstract method from a base class (polymorphism — rename the whole interface together or not at all).
- Framework hooks implementing an external protocol: `json.JSONEncoder.default`, `threading.Thread.run`, importlib's `find_spec`/`create_module`/`exec_module`, a `warnings` `*_formatwarning`.
- **Vendored / third-party-API-mirroring code** (e.g. the KServe adapters under `client/istio/`, generated `*_pb2*.py`). This mirrors an upstream contract; renaming it diverges from upstream and breaks re-vendoring. Treat it like generated code and leave it alone.

The `cli/` and `mcp/` modules are out of scope for this rule.

## Marking Public Entities

### `@public`

```python
from hopsworks_common.decorators import public  # re-exported from hopsworks_apigen

@public("hopsworks.FeatureStore", "hsfs.FeatureStore")
class FeatureStore:
    """..."""
```

Arguments are the namespace paths under which the entity appears in the documentation.
An entity may appear under multiple namespaces.

### `@also_available_as`

Used when the same entity is accessible under multiple names in different packages:

```python
@public("hsfs.feature_group.FeatureGroup")
@also_available_as("hopsworks.FeatureGroup")
class FeatureGroup:
    ...
```

### `@deprecated`

```python
from hopsworks_common.decorators import deprecated

@deprecated("hsfs.feature.Feature.isin", available_until="5.0")
def contains(self, other: str | list) -> filter.Filter:
    """Construct a filter similar to SQL's `IN` operator.

    Parameters:
        other: A single feature value or a list of feature values.

    Returns:
        A filter that leaves only the feature values also contained in `other`.
    """
    return self.isin(other)
```

The first positional arguments are the replacement paths (at least one required).
`available_until` is the first release in which the entity will become unavailable; include it when the removal release is known.
The decorator adds a deprecation warning at call time and into the docs.
The docstring must name the replacement.

## Linking in Docstrings

Link to any entity mentioned by name.
Use full module paths.

```markdown
[`FeatureGroup`][hsfs.feature_group.FeatureGroup]
[`FeatureGroup.insert`][hsfs.feature_group.FeatureGroup.insert]
```

Include the class name when linking methods to avoid ambiguity.
External links use plain Markdown syntax.

## Documenting New API Surfaces

When adding a new public class or module:

- Annotate with `@public(...)` and write a full docstring

## Patch Notes

When behavior changes in a patch release, add an `Info:` admonition to the affected docstring:

```python
"""Does something.

Info: Accepts polars DataFrames, ~=4.8.1
    Starting from version 4.8.1, this method accepts polars DataFrames in addition
    to pandas DataFrames.
    Pass a polars DataFrame directly; no conversion is needed.
"""
```

The `~=X.Y.Z` suffix in the title signals applicability from that version onward.
