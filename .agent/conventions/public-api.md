# Public API

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
